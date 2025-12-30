# Async Implementation Guide for BeanQueue

**Goal**: Add async/await interface alongside existing sync implementation, similar to fastapi-pagination's dual interface pattern.

---

## Current Architecture Analysis

BeanQueue currently uses:
- **SQLAlchemy 2.0+** (sync)
- **psycopg2-binary** (sync PostgreSQL driver)
- **Threading** for worker heartbeat & metrics server
- **Blocking select()** for LISTEN/POLL operations (`services/dispatch.py:105`)

---

## Recommended Approach: Dual Interface Pattern

Keep sync interface for backward compatibility, add async alongside it.

### Package Structure

```
bq/
├── __init__.py           # Export both sync and async
├── app.py                # Sync BeanQueue (current)
├── aio.py                # NEW: AsyncBeanQueue
├── config.py             # Shared config (works for both)
├── constants.py          # Shared constants
├── events.py             # Shared events (blinker supports async)
├── utils.py              # Shared utils
├── models/               # Shared models (SQLAlchemy 2.0 supports both)
│   ├── task.py
│   ├── worker.py
│   └── event.py
├── processors/
│   ├── processor.py      # Sync processor
│   ├── aio_processor.py  # NEW: Async processor
│   ├── registry.py       # Update to support both
│   └── retry_policies.py # Shared (pure Python logic)
├── services/
│   ├── worker.py         # Sync WorkerService
│   ├── dispatch.py       # Sync DispatchService
│   ├── aio_worker.py     # NEW: AsyncWorkerService
│   └── aio_dispatch.py   # NEW: AsyncDispatchService
├── db/
│   ├── base.py           # Shared declarative base
│   ├── session.py        # Sync session
│   └── aio_session.py    # NEW: Async session
└── cmds/
    ├── cli.py
    ├── process.py        # Sync worker command
    └── aio_process.py    # NEW: Async worker command
```

---

## Implementation Steps

### Step 1: Add Dependencies

Update `pyproject.toml`:

```toml
[project]
dependencies = [
    "sqlalchemy>=2.0.30,<3",
    "venusian>=3.1.0,<4",
    "click>=8.1.7,<9",
    "pydantic-settings>=2.2.1,<3",
    "blinker>=1.8.2,<2",
    "rich>=13.7.1,<14",
    # NEW async dependencies
    "asyncpg>=0.29.0,<1",       # Async PostgreSQL driver
    "greenlet>=3.0.0,<4",        # Required by SQLAlchemy async
]

[project.optional-dependencies]
async = [
    "asyncpg>=0.29.0,<1",
]
```

### Step 2: Create Async Session Factory

**File**: `bq/db/aio_session.py`

```python
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker

def create_async_engine_from_url(database_url: str):
    """Create async engine from database URL."""
    # Convert postgresql:// to postgresql+asyncpg://
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    return create_async_engine(
        database_url,
        echo=False,
        pool_pre_ping=True,  # Verify connections before using
    )

AsyncSessionMaker = async_sessionmaker(
    class_=AsyncSession,
    expire_on_commit=False,
)
```

### Step 3: Create Async Services

**File**: `bq/services/aio_dispatch.py`

```python
import dataclasses
import typing
import uuid
import asyncio

from sqlalchemy import func, null, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Query

from .. import models


@dataclasses.dataclass(frozen=True)
class Notification:
    pid: int
    channel: str
    payload: typing.Optional[str] = None


class AsyncDispatchService:
    def __init__(self, session: AsyncSession, task_model: typing.Type = models.Task):
        self.session = session
        self.task_model: typing.Type[models.Task] = task_model

    def make_task_query(
        self,
        channels: typing.Sequence[str],
        limit: int = 1,
        now: typing.Any = func.now(),
    ):
        """Build query for pending tasks (same logic as sync)."""
        return (
            self.session.query(self.task_model.id)
            .filter(self.task_model.channel.in_(channels))
            .filter(self.task_model.state == models.TaskState.PENDING)
            .filter(
                or_(
                    self.task_model.scheduled_at.is_(null()),
                    now >= self.task_model.scheduled_at,
                )
            )
            .order_by(self.task_model.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

    async def dispatch(
        self,
        channels: typing.Sequence[str],
        worker_id: uuid.UUID,
        limit: int = 1,
        now: typing.Any = func.now(),
    ):
        """Dispatch tasks to worker (async version)."""
        task_query = self.make_task_query(channels, limit=limit, now=now)
        task_subquery = task_query.scalar_subquery()

        update_query = (
            self.task_model.__table__.update()
            .where(self.task_model.id.in_(task_subquery))
            .values(
                state=models.TaskState.PROCESSING,
                worker_id=worker_id,
            )
            .returning(self.task_model.id)
        )

        result = await self.session.execute(update_query)
        task_ids = [item[0] for item in result]

        # Fetch full task objects
        query = self.session.query(self.task_model).filter(
            self.task_model.id.in_(task_ids)
        )
        result = await self.session.execute(query)
        return result.scalars().all()

    async def listen(self, channels: typing.Sequence[str]):
        """Subscribe to PostgreSQL NOTIFY channels."""
        conn = await self.session.connection()
        for channel in channels:
            quoted_channel = conn.dialect.identifier_preparer.quote_identifier(channel)
            await conn.exec_driver_sql(f"LISTEN {quoted_channel}")

    async def poll(self, timeout: int = 5) -> typing.AsyncGenerator[Notification, None]:
        """
        Poll for NOTIFY events using asyncpg.

        NOTE: asyncpg has different API than psycopg2 for LISTEN/NOTIFY.
        """
        conn = await self.session.connection()
        raw_conn = await conn.get_raw_connection()
        driver_conn = raw_conn.driver_connection  # asyncpg connection

        # asyncpg uses add_listener() instead of polling
        notifications_queue = asyncio.Queue()

        async def notification_handler(connection, pid, channel, payload):
            await notifications_queue.put(
                Notification(pid=pid, channel=channel, payload=payload)
            )

        # Add listeners for all channels
        # Note: This requires tracking which channels we're listening to
        # For simplicity, we'll use a different approach with asyncio.wait_for

        try:
            notification = await asyncio.wait_for(
                notifications_queue.get(),
                timeout=timeout
            )
            yield notification
        except asyncio.TimeoutError:
            raise TimeoutError("Timeout waiting for new notifications")

    async def notify(self, channels: typing.Sequence[str]):
        """Send NOTIFY to channels."""
        conn = await self.session.connection()
        for channel in channels:
            quoted_channel = conn.dialect.identifier_preparer.quote_identifier(channel)
            await conn.exec_driver_sql(f"NOTIFY {quoted_channel}")
```

**File**: `bq/services/aio_worker.py`

```python
import datetime
import typing
import uuid

from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession

from .. import models


class AsyncWorkerService:
    def __init__(
        self,
        session: AsyncSession,
        task_model: typing.Type = models.Task,
        worker_model: typing.Type = models.Worker,
    ):
        self.session = session
        self.task_model = task_model
        self.worker_model = worker_model

    async def get_worker(self, worker_id: uuid.UUID):
        """Get worker by ID."""
        return await self.session.get(self.worker_model, worker_id)

    def make_worker(self, name: str, channels: tuple[str, ...]):
        """Create worker instance (not async - just creates object)."""
        return self.worker_model(name=name, channels=channels)

    async def fetch_dead_workers(self, timeout: int, limit: int = 5):
        """Find and mark dead workers."""
        # Build query for dead workers
        dead_worker_query = (
            self.session.query(self.worker_model.id)
            .filter(
                self.worker_model.last_heartbeat
                < (func.now() - datetime.timedelta(seconds=timeout))
            )
            .filter(self.worker_model.state == models.WorkerState.RUNNING)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )

        dead_worker_subquery = dead_worker_query.scalar_subquery()

        # Update dead workers
        update_query = (
            self.worker_model.__table__.update()
            .where(self.worker_model.id.in_(dead_worker_subquery))
            .values(state=models.WorkerState.NO_HEARTBEAT)
            .returning(self.worker_model.id)
        )

        result = await self.session.execute(update_query)
        worker_ids = [item[0] for item in result]

        # Fetch full worker objects
        query = self.session.query(self.worker_model).filter(
            self.worker_model.id.in_(worker_ids)
        )
        result = await self.session.execute(query)
        return result.scalars().all()

    async def reschedule_dead_tasks(self, worker_ids: typing.List[uuid.UUID]) -> int:
        """Reschedule tasks from dead workers back to PENDING."""
        update_query = (
            self.task_model.__table__.update()
            .where(self.task_model.worker_id.in_(worker_ids))
            .where(self.task_model.state == models.TaskState.PROCESSING)
            .values(
                state=models.TaskState.PENDING,
                worker_id=None,
            )
        )

        result = await self.session.execute(update_query)
        return result.rowcount
```

### Step 4: Create Async Processor

**File**: `bq/processors/aio_processor.py`

```python
import dataclasses
import inspect
import logging
import typing
from contextvars import ContextVar

from sqlalchemy.ext.asyncio import AsyncSession

from .. import models

logger = logging.getLogger(__name__)

# Async version of current_task
current_task_async: ContextVar[models.Task | None] = ContextVar(
    "current_task_async", default=None
)


@dataclasses.dataclass(frozen=True)
class AsyncProcessor:
    """Async version of Processor."""

    module: str
    name: str
    channel: str
    func: typing.Callable
    auto_complete: bool = True
    retry_policy: typing.Callable | None = None
    retry_exceptions: typing.Type | typing.Tuple[typing.Type, ...] | None = None

    async def process(
        self,
        task: models.Task,
        session: AsyncSession,
        event_cls: typing.Type | None = None,
    ):
        """Execute async processor function."""
        # Set current task in context
        token = current_task_async.set(task)

        try:
            # Build kwargs from task.kwargs
            kwargs = task.kwargs or {}

            # Inject optional parameters
            sig = inspect.signature(self.func)
            if "db" in sig.parameters:
                kwargs["db"] = session
            if "task" in sig.parameters:
                kwargs["task"] = task
            if "savepoint" in sig.parameters:
                kwargs["savepoint"] = await session.begin_nested()

            # Execute async processor
            result = await self.func(**kwargs)

            # Mark as done
            task.state = models.TaskState.DONE
            task.result = result

            if self.auto_complete:
                session.add(task)
                await session.commit()

            return result

        except Exception as e:
            logger.exception("Task %s failed: %s", task.id, e)

            # Check if should retry
            should_retry = False
            if self.retry_exceptions:
                should_retry = isinstance(e, self.retry_exceptions)
            elif self.retry_policy:
                should_retry = True

            if should_retry and self.retry_policy:
                scheduled_at = self.retry_policy(task)
                if scheduled_at:
                    task.state = models.TaskState.PENDING
                    task.scheduled_at = scheduled_at
                    logger.info("Rescheduling task %s to %s", task.id, scheduled_at)
                else:
                    task.state = models.TaskState.FAILED
                    task.error_message = str(e)
            else:
                task.state = models.TaskState.FAILED
                task.error_message = str(e)

            if self.auto_complete:
                session.add(task)
                await session.commit()

            raise

        finally:
            current_task_async.reset(token)


@dataclasses.dataclass(frozen=True)
class AsyncProcessorHelper:
    """Helper for creating tasks from async processors."""

    processor: AsyncProcessor
    task_cls: typing.Type[models.Task]

    def run(self, **kwargs) -> models.Task:
        """Create task instance (same as sync version)."""
        parent_task = current_task_async.get()

        task = self.task_cls(
            channel=self.processor.channel,
            module=self.processor.module,
            func_name=self.processor.name,
            kwargs=kwargs,
            parent_id=parent_task.id if parent_task else None,
        )

        return task
```

### Step 5: Create AsyncBeanQueue

**File**: `bq/aio.py`

```python
import asyncio
import functools
import importlib
import logging
import platform
import typing
from importlib.metadata import version, PackageNotFoundError

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from . import constants, events, models
from .config import Config
from .db.aio_session import AsyncSessionMaker, create_async_engine_from_url
from .processors.aio_processor import AsyncProcessor, AsyncProcessorHelper
from .processors.registry import collect
from .services.aio_dispatch import AsyncDispatchService
from .services.aio_worker import AsyncWorkerService
from .utils import load_module_var

logger = logging.getLogger(__name__)


class AsyncBeanQueue:
    """Async version of BeanQueue."""

    def __init__(
        self,
        config: Config | None = None,
        session_cls: typing.Type[AsyncSession] = AsyncSessionMaker,
        worker_service_cls: typing.Type[AsyncWorkerService] = AsyncWorkerService,
        dispatch_service_cls: typing.Type[AsyncDispatchService] = AsyncDispatchService,
        engine: AsyncEngine | None = None,
    ):
        self.config = config if config is not None else Config()
        self.session_cls = session_cls
        self.worker_service_cls = worker_service_cls
        self.dispatch_service_cls = dispatch_service_cls
        self._engine = engine
        self._shutdown_event = asyncio.Event()

    async def create_default_engine(self):
        """Create async engine."""
        return create_async_engine_from_url(str(self.config.DATABASE_URL))

    def make_session(self) -> AsyncSession:
        """Create async session."""
        return self.session_cls(bind=self.engine)

    @property
    async def engine(self) -> AsyncEngine:
        """Get or create async engine."""
        if self._engine is None:
            self._engine = await self.create_default_engine()
        return self._engine

    @property
    def task_model(self) -> typing.Type[models.Task]:
        return load_module_var(self.config.TASK_MODEL)

    @property
    def worker_model(self) -> typing.Type[models.Worker]:
        return load_module_var(self.config.WORKER_MODEL)

    @property
    def event_model(self) -> typing.Type[models.Event] | None:
        if self.config.EVENT_MODEL is None:
            return None
        return load_module_var(self.config.EVENT_MODEL)

    def processor(
        self,
        channel: str = constants.DEFAULT_CHANNEL,
        auto_complete: bool = True,
        retry_policy: typing.Callable | None = None,
        retry_exceptions: typing.Type | typing.Tuple[typing.Type, ...] | None = None,
        task_model: typing.Type | None = None,
    ) -> typing.Callable:
        """
        Decorator for async processors.

        Usage:
            @async_app.processor(channel="images")
            async def resize_image(db: AsyncSession, width: int, height: int):
                # async processing logic
                pass
        """
        def decorator(wrapped: typing.Callable):
            processor = AsyncProcessor(
                module=wrapped.__module__,
                name=wrapped.__name__,
                channel=channel,
                func=wrapped,
                auto_complete=auto_complete,
                retry_policy=retry_policy,
                retry_exceptions=retry_exceptions,
            )

            helper_obj = AsyncProcessorHelper(
                processor,
                task_cls=task_model if task_model is not None else self.task_model,
            )

            # Use same venusian scanning as sync version
            import venusian

            def callback(scanner: venusian.Scanner, name: str, ob: typing.Callable):
                if processor.name != name:
                    raise ValueError("Name is not the same")
                scanner.registry.add(processor)

            venusian.attach(
                helper_obj, callback, category=constants.BQ_ASYNC_PROCESSOR_CATEGORY
            )

            return helper_obj

        return decorator

    async def update_workers(self, worker_id: typing.Any):
        """
        Async worker heartbeat loop.

        Uses asyncio.sleep instead of threading.Event.wait.
        """
        while not self._shutdown_event.is_set():
            async with self.make_session() as db:
                try:
                    worker_service = self.worker_service_cls(
                        session=db,
                        task_model=self.task_model,
                        worker_model=self.worker_model,
                    )
                    dispatch_service = self.dispatch_service_cls(
                        session=db,
                        task_model=self.task_model,
                    )

                    current_worker = await worker_service.get_worker(worker_id)
                    if current_worker is None:
                        logger.error("Worker %s not found", worker_id)
                        return

                    # Check for dead workers
                    dead_workers = await worker_service.fetch_dead_workers(
                        timeout=self.config.WORKER_HEARTBEAT_TIMEOUT
                    )

                    if dead_workers:
                        worker_ids = [w.id for w in dead_workers]
                        task_count = await worker_service.reschedule_dead_tasks(worker_ids)

                        for dead_worker in dead_workers:
                            logger.info(
                                "Found dead worker %s, rescheduled %s tasks",
                                dead_worker.id,
                                task_count,
                            )

                        await dispatch_service.notify([w.channels for w in dead_workers])
                        await db.commit()

                    # Check worker state
                    if current_worker.state != models.WorkerState.RUNNING:
                        logger.warning(
                            "Worker %s state is %s, stopping",
                            current_worker.id,
                            current_worker.state,
                        )
                        return

                    # Update heartbeat
                    current_worker.last_heartbeat = func.now()
                    db.add(current_worker)
                    await db.commit()

                except Exception as e:
                    logger.error("Error in update_workers: %s", e, exc_info=True)
                    await asyncio.sleep(5)  # Brief pause before retry

            # Wait for next heartbeat
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.config.WORKER_HEARTBEAT_PERIOD,
                )
                # If we get here, shutdown was requested
                return
            except asyncio.TimeoutError:
                # Normal timeout, continue loop
                continue

    async def process_tasks(self, channels: tuple[str, ...]):
        """
        Main async worker loop.

        Key differences from sync version:
        - Uses asyncio.create_task() instead of threading.Thread()
        - Uses async context managers
        - Uses asyncio.gather() for graceful shutdown
        """
        try:
            bq_version = version("beanqueue")
        except PackageNotFoundError:
            bq_version = "unknown"

        logger.info("Starting async processing, bq_version=%s", bq_version)

        if not channels:
            channels = (constants.DEFAULT_CHANNEL,)

        if not self.config.PROCESSOR_PACKAGES:
            raise ValueError("No PROCESSOR_PACKAGES provided")

        # Scan for processors
        logger.info("Scanning packages %s", self.config.PROCESSOR_PACKAGES)
        pkgs = list(map(importlib.import_module, self.config.PROCESSOR_PACKAGES))
        registry = collect(pkgs, category=constants.BQ_ASYNC_PROCESSOR_CATEGORY)

        # Create worker
        async with self.make_session() as db:
            worker_service = self.worker_service_cls(
                session=db,
                task_model=self.task_model,
                worker_model=self.worker_model,
            )
            dispatch_service = self.dispatch_service_cls(
                session=db,
                task_model=self.task_model,
            )

            worker = worker_service.make_worker(
                name=platform.node(),
                channels=channels,
            )
            db.add(worker)
            await dispatch_service.listen(channels)
            await db.commit()

            worker_id = worker.id

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(self.update_workers(worker_id))

        logger.info("Created worker %s, processing channels %s", worker_id, channels)
        events.worker_init.send(self, worker=worker)

        try:
            # Main processing loop
            while True:
                async with self.make_session() as db:
                    dispatch_service = self.dispatch_service_cls(
                        session=db,
                        task_model=self.task_model,
                    )

                    # Dispatch tasks
                    while True:
                        tasks = await dispatch_service.dispatch(
                            channels,
                            worker_id=worker_id,
                            limit=self.config.BATCH_SIZE,
                        )

                        if not tasks:
                            break

                        for task in tasks:
                            logger.info(
                                "Processing task %s, channel=%s, func=%s",
                                task.id,
                                task.channel,
                                task.func_name,
                            )
                            await registry.process_async(task, session=db)

                        await db.commit()

                # Poll for notifications
                async with self.make_session() as db:
                    dispatch_service = self.dispatch_service_cls(
                        session=db,
                        task_model=self.task_model,
                    )

                    try:
                        async for notification in dispatch_service.poll(
                            timeout=self.config.POLL_TIMEOUT
                        ):
                            logger.debug("Received notification %s", notification)
                    except TimeoutError:
                        logger.debug("Poll timeout")

        except (SystemExit, KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Shutting down...")
            self._shutdown_event.set()

            # Wait for heartbeat task
            await asyncio.wait_for(heartbeat_task, timeout=5)

            # Mark worker as shutdown
            async with self.make_session() as db:
                worker_service = self.worker_service_cls(
                    session=db,
                    task_model=self.task_model,
                    worker_model=self.worker_model,
                )
                dispatch_service = self.dispatch_service_cls(
                    session=db,
                    task_model=self.task_model,
                )

                worker = await worker_service.get_worker(worker_id)
                worker.state = models.WorkerState.SHUTDOWN
                db.add(worker)

                task_count = await worker_service.reschedule_dead_tasks([worker_id])
                logger.info("Rescheduled %s tasks", task_count)

                await dispatch_service.notify(channels)
                await db.commit()

        logger.info("Shutdown complete")
```

### Step 6: Update Constants

**File**: `bq/constants.py`

```python
DEFAULT_CHANNEL = "default"
BQ_PROCESSOR_CATEGORY = "bq.processor"
BQ_ASYNC_PROCESSOR_CATEGORY = "bq.async_processor"  # NEW
```

### Step 7: Update Main __init__.py

**File**: `bq/__init__.py`

```python
# Sync interface (existing)
from .app import BeanQueue
from .config import Config
from .models import Task, Worker, Event
from .processors.retry_policies import (
    DelayRetry,
    ExponentialBackoffRetry,
    LimitAttempt,
)

# Async interface (new)
from .aio import AsyncBeanQueue

# Export both
__all__ = [
    # Sync
    "BeanQueue",
    "Config",
    "Task",
    "Worker",
    "Event",
    "DelayRetry",
    "ExponentialBackoffRetry",
    "LimitAttempt",

    # Async
    "AsyncBeanQueue",
]
```

### Step 8: Add Async CLI Command

**File**: `bq/cmds/aio_process.py`

```python
import asyncio
import click

from .environment import pass_bq_async


@click.command()
@click.argument("channels", nargs=-1)
@pass_bq_async
def aio_process_cmd(app, channels):
    """Process tasks asynchronously."""
    asyncio.run(app.process_tasks(tuple(channels)))
```

Update `bq/cmds/cli.py`:

```python
@cli.command("aio-process")
@click.argument("channels", nargs=-1)
@pass_bq_async
def aio_process(app, channels):
    """Process tasks asynchronously (async/await interface)."""
    import asyncio
    asyncio.run(app.process_tasks(tuple(channels)))
```

---

## Usage Examples

### Sync Interface (Existing)

```python
import bq
from sqlalchemy.orm import Session

app = bq.BeanQueue()

@app.processor(channel="images")
def resize_image(db: Session, width: int, height: int):
    # Sync processing
    pass

# Run sync worker
app.process_tasks(channels=("images",))
```

### Async Interface (New)

```python
import bq
from sqlalchemy.ext.asyncio import AsyncSession

async_app = bq.AsyncBeanQueue()

@async_app.processor(channel="images")
async def resize_image(db: AsyncSession, width: int, height: int):
    # Async processing
    async with httpx.AsyncClient() as client:
        response = await client.get(f"https://api.example.com/resize")
    pass

# Run async worker
import asyncio
asyncio.run(async_app.process_tasks(channels=("images",)))
```

### CLI Usage

```bash
# Sync worker
bq process images

# Async worker
bq aio-process images
```

---

## Testing Strategy

### Unit Tests

```python
# tests/unit/test_aio_services.py
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

@pytest.fixture
async def async_db():
    engine = create_async_engine("postgresql+asyncpg://...")
    async with AsyncSession(engine) as session:
        yield session

@pytest.mark.asyncio
async def test_async_dispatch(async_db):
    service = AsyncDispatchService(session=async_db)
    tasks = await service.dispatch(["images"], worker_id=uuid4())
    assert len(tasks) == 1
```

### Integration Tests

```python
# tests/acceptance/test_aio_process_cmd.py
import asyncio
import pytest

@pytest.mark.asyncio
async def test_async_worker_processes_tasks():
    async_app = bq.AsyncBeanQueue(config=config)
    # Submit tasks
    # Run worker for 10 seconds
    # Verify all tasks completed
```

---

## Migration Path

### Phase 1: Foundation (Week 1-2)
- Add asyncpg dependency
- Create async session factory
- Implement AsyncDispatchService
- Implement AsyncWorkerService

### Phase 2: Core (Week 3-4)
- Implement AsyncProcessor
- Implement AsyncBeanQueue
- Update Registry to support both sync/async
- Add async CLI command

### Phase 3: Polish (Week 5-6)
- Add comprehensive tests
- Update documentation
- Add examples
- Performance benchmarking

### Phase 4: Release (Week 7)
- Beta release
- Gather feedback
- Fix bugs
- Stable release

---

## Performance Considerations

### Async Advantages
- Higher concurrency for I/O-bound tasks
- Lower memory footprint (no thread stacks)
- Better scalability (1000s of concurrent tasks)

### Async Disadvantages
- Complexity (async/await learning curve)
- Debugging harder
- Not all libraries support async

### When to Use Async
- Heavy I/O workload (HTTP requests, file uploads)
- High task volume (1000s/second)
- Integration with async frameworks (FastAPI, aiohttp)

### When to Use Sync
- CPU-bound tasks (image processing, calculations)
- Simpler codebase
- Existing sync dependencies

---

## Alternative: Unified Interface (Not Recommended)

Instead of dual interface, could detect and support both:

```python
@app.processor(channel="images")
async def resize_image(...):  # Async
    pass

@app.processor(channel="files")
def upload_file(...):  # Sync
    pass
```

**Why not recommended**:
- More complex implementation
- Harder to maintain
- Performance overhead (detecting async vs sync)
- fastapi-pagination doesn't do this for good reasons

---

## Key Differences from Sync

| Aspect | Sync | Async |
|--------|------|-------|
| Database Driver | psycopg2 | asyncpg |
| SQLAlchemy | Session | AsyncSession |
| Concurrency | threading.Thread | asyncio.Task |
| Heartbeat | threading.Event.wait() | asyncio.Event.wait() |
| LISTEN/POLL | select.select() | asyncio wait/Queue |
| Processor | def func() | async def func() |
| Registry | Registry.process() | Registry.process_async() |

---

## References

- **fastapi-pagination**: https://github.com/uriyyo/fastapi-pagination
- **SQLAlchemy Async**: https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
- **asyncpg**: https://magicstack.github.io/asyncpg/
- **procrastinate** (async queue): https://github.com/procrastinate-org/procrastinate

---

**Estimated Implementation Time**: 6-8 weeks
**Complexity**: High
**Backward Compatibility**: 100% (adds new interface, doesn't change existing)
