# CLAUDE.md - BeanQueue AI Assistant Guide

This document provides comprehensive guidance for AI assistants working on the BeanQueue (bq) codebase. Last updated: 2025-11-23

## Project Overview

**BeanQueue** is a lightweight Python task queue framework (~1000 lines) built on PostgreSQL, SQLAlchemy, SKIP LOCKED queries, and NOTIFY/LISTEN. It enables transactional task processing by storing tasks directly in the database.

- **Version**: 1.1.9
- **Language**: Python 3.11+
- **Package Manager**: uv (astral-sh/uv)
- **License**: MIT
- **Primary Use Case**: Background task processing that needs to be transactional with database operations
- **Sponsor**: BeanHub.io (accounting SaaS)

### Core Philosophy

BeanQueue solves the **transactionality problem** of task queues. Unlike external message brokers (RabbitMQ, Redis), BeanQueue stores tasks in PostgreSQL, ensuring tasks and related data are committed atomically in the same transaction. This eliminates race conditions where:
- Tasks execute before database transactions commit
- Database commits fail after tasks are enqueued
- Data inconsistencies between task queue and database

## Architecture & Design Patterns

### Layered Architecture

```
CLI (bq/cmds/)
    ↓
App (bq/app.py)
    ↓
Processors (bq/processors/)
    ↓
Services (bq/services/)
    ↓
Models (bq/models/)
    ↓
PostgreSQL Database
```

### Key Design Patterns

1. **Decorator Pattern** (`@app.processor`)
   - Uses venusian for runtime discovery
   - Converts functions into Processor objects with metadata
   - Returns ProcessorHelper for fluent task creation

2. **Registry Pattern**
   - `Registry` class organizes processors by `channel → module → func_name`
   - `venusian.Scanner` scans packages at startup
   - Dynamic lookup during task processing

3. **Mixin Pattern** (Models)
   - `TaskModelMixin`, `WorkerModelMixin`, `EventModelMixin` - Core columns
   - `TaskModelRefWorkerMixin`, `WorkerRefMixin`, etc. - Relationships
   - Users can compose custom models by inheriting mixins

4. **Service Layer**
   - `WorkerService`: Worker CRUD, heartbeat management, dead worker detection
   - `DispatchService`: Task polling, SKIP LOCKED queries, NOTIFY/LISTEN

5. **Event System** (blinker signals)
   - `worker_init`: When worker starts
   - `task_failure`: When task fails
   - Extensibility for custom behavior

6. **Context Variables**
   - `current_task` (contextvars.ContextVar) for accessing current task in processors
   - Thread-safe, enables subtask parent tracking

## Directory Structure

```
/home/user/bq/
├── bq/                          # Main source (981 lines)
│   ├── __init__.py              # Public API exports
│   ├── app.py                   # BeanQueue class (384 lines)
│   ├── config.py                # Config class with pydantic-settings
│   ├── constants.py             # DEFAULT_CHANNEL, etc.
│   ├── events.py                # blinker signal definitions
│   ├── utils.py                 # load_module_var, etc.
│   ├── cmds/                    # CLI commands (Click)
│   │   ├── cli.py               # Main CLI group
│   │   ├── process.py           # `bq process` command
│   │   ├── submit.py            # `bq submit` command
│   │   ├── create_tables.py     # `bq create_tables` command
│   │   ├── environment.py       # CLI context management
│   │   ├── utils.py             # CLI utilities
│   │   └── main.py              # Entry point
│   ├── db/
│   │   ├── base.py              # SQLAlchemy declarative base
│   │   └── session.py           # Session factory
│   ├── models/
│   │   ├── task.py              # Task model + NOTIFY event listener (178 lines)
│   │   ├── worker.py            # Worker model (79 lines)
│   │   ├── event.py             # Event model (76 lines)
│   │   └── helpers.py           # Model utilities
│   ├── processors/
│   │   ├── processor.py         # Processor class (120 lines)
│   │   ├── registry.py          # Registry + collect function (57 lines)
│   │   ├── retry_policies.py    # DelayRetry, ExponentialBackoffRetry, LimitAttempt
│   │   └── __init__.py
│   └── services/
│       ├── worker.py            # WorkerService (83 lines)
│       └── dispatch.py          # DispatchService (117 lines)
├── tests/
│   ├── conftest.py              # Pytest fixtures (db, engine, session)
│   ├── factories.py             # Factory Boy models
│   ├── unit/                    # Unit tests
│   │   ├── processors/
│   │   ├── services/
│   │   └── fixtures/
│   └── acceptance/              # Integration tests
│       ├── test_process_cmd.py  # E2E test (10 workers, 1000 tasks)
│       └── fixtures/
├── pyproject.toml               # Package metadata, dependencies
├── uv.lock                      # Dependency lock file
├── docker-compose.yaml          # PostgreSQL 16.3 for testing
├── .pre-commit-config.yaml      # ruff-format + reorder-python-imports
├── .circleci/config.yml         # CI/CD (test + publish to PyPI)
├── README.md                    # Comprehensive user documentation
└── LICENSE                      # MIT License
```

## Data Models

### Task Model (`bq_tasks` table)

```python
- id: UUID (primary key)
- state: Enum (PENDING, PROCESSING, DONE, FAILED)
- channel: String (for LISTEN/NOTIFY, indexed)
- module: String (processor module path)
- func_name: String (processor function name)
- kwargs: JSONB (task arguments)
- result: JSONB (task result after completion)
- error_message: String (failure reason)
- created_at: DateTime
- scheduled_at: DateTime (nullable, for delayed execution)
- worker_id: UUID (foreign key → Worker)
- parent_id: UUID (foreign key → Task, for subtasks)
```

**Relationships**: `worker`, `events`, `parent`, `children`

### Worker Model (`bq_workers` table)

```python
- id: UUID (primary key)
- state: Enum (RUNNING, SHUTDOWN, NO_HEARTBEAT)
- name: String (default: hostname)
- channels: ARRAY(String) (which channels to process)
- last_heartbeat: DateTime (indexed, updated every 30s)
- created_at: DateTime
```

**Relationships**: `tasks`

### Event Model (`bq_events` table)

```python
- id: UUID (primary key)
- type: Enum (FAILED, FAILED_RETRY_SCHEDULED, COMPLETE)
- error_message: String
- scheduled_at: DateTime (for retry scheduling)
- created_at: DateTime
- task_id: UUID (foreign key → Task)
```

**Relationships**: `task`

## Task Processing Flow

1. **Task Creation** (state=PENDING)
   - Insert Task into database
   - SQLAlchemy event listener triggers `NOTIFY <channel>`
   - Transaction commit makes task visible to workers

2. **Worker Polls** (`DispatchService.dispatch`)
   - `SELECT ... FOR UPDATE SKIP LOCKED` (N pending tasks)
   - Updates `state → PROCESSING`, assigns `worker_id`
   - Returns task objects for processing

3. **Processor Execution**
   - Looks up Processor in Registry (by `channel/module/func_name`)
   - Calls processor function with params + optional `db/task/savepoint`
   - **Success**: `state → DONE`, stores `result` in JSONB
   - **Failure**: `state → FAILED`, stores `error_message`
   - **Retry**: Retry policy can reschedule with `scheduled_at`

4. **Worker Heartbeat** (background thread)
   - Every 30s: updates `last_heartbeat`
   - Detects dead workers (no heartbeat > 100s)
   - Reschedules their tasks back to `PENDING`
   - Triggers `NOTIFY` for affected channels

5. **Database Polling** (`DispatchService.poll`)
   - Waits for `NOTIFY` on subscribed channels with timeout
   - Wakes up on notification or timeout
   - Returns to dispatch loop

## Configuration

All configuration via environment variables with `BQ_` prefix or programmatic `Config` object.

### Key Environment Variables

```bash
# Processor Discovery
BQ_PROCESSOR_PACKAGES='["my_pkgs.processors"]'  # JSON list of packages to scan

# Database
BQ_DATABASE_URL="postgresql://user:pass@localhost/bq"
# OR individual components:
BQ_POSTGRES_SERVER="localhost"
BQ_POSTGRES_USER="bq"
BQ_POSTGRES_PASSWORD=""
BQ_POSTGRES_DB="bq"

# Processing Behavior
BQ_BATCH_SIZE=1                     # Tasks to fetch per dispatch
BQ_POLL_TIMEOUT=60                  # Seconds to wait before polling again
BQ_WORKER_HEARTBEAT_PERIOD=30       # Worker update interval (seconds)
BQ_WORKER_HEARTBEAT_TIMEOUT=100     # Worker timeout detection (seconds)

# Custom Models
BQ_TASK_MODEL="my_pkgs.models.Task"
BQ_WORKER_MODEL="my_pkgs.models.Worker"
BQ_EVENT_MODEL="my_pkgs.models.Event"

# Metrics HTTP Server
BQ_METRICS_HTTP_SERVER_ENABLED=True
BQ_METRICS_HTTP_SERVER_PORT=8000
```

### Programmatic Configuration

```python
import bq
config = bq.Config(
    PROCESSOR_PACKAGES=["my_pkgs.processors"],
    DATABASE_URL="postgresql://localhost/bq",
    BATCH_SIZE=10,
)
app = bq.BeanQueue(config=config)
```

## Development Workflow

### Package Manager: uv

```bash
# Install dependencies
pip install uv
uv sync

# Run tests
uv run python -m pytest ./tests -svvvv

# Build package
uv build

# Publish to PyPI
uv publish
```

### Code Style & Pre-commit Hooks

**Tools**:
- `ruff-format` (v0.11.6) - Code formatter
- `reorder-python-imports` (v3.10.0) - Import ordering

**Run manually**:
```bash
pre-commit run --all-files
```

**Import Order**:
1. Standard library
2. Third-party packages
3. Local imports (relative imports)

### CLI Commands

```bash
# Create database tables
bq create_tables

# Process tasks (requires BQ_PROCESSOR_PACKAGES)
bq process images

# Process with custom app
bq -a my_pkgs.bq.app process images

# Process with debug logging
bq -l debug process images

# Submit a task (for testing)
bq submit images my_module.resize_image -k '{"width": 200, "height": 300}'

# Help
bq --help
```

### Testing

**Setup**:
```bash
# Start PostgreSQL via Docker Compose
docker-compose up -d

# Run tests
uv run python -m pytest ./tests -svvvv
```

**Test Structure**:
- `tests/unit/` - Isolated unit tests (processors, services, config)
- `tests/acceptance/` - Integration tests with real database
- `tests/conftest.py` - Pytest fixtures (db, engine, session)
- `tests/factories.py` - Factory Boy models for test data

**Key Fixtures** (from `conftest.py`):
- `db_url` - PostgreSQL connection string (from `TEST_DB_URL` env var)
- `engine` - SQLAlchemy engine
- `db` - Session with auto create/drop tables
- `task`, `worker`, `event` - Factory fixtures (pytest-factoryboy)

**Example Acceptance Test**:
`test_process_cmd.py` spawns 10 worker processes, submits 1000 tasks, verifies all complete within 30 seconds.

### CI/CD (CircleCI)

**Test Job**:
- Python 3.11.12 + PostgreSQL 16.2
- Runs on every commit and tag
- Command: `uv run python -m pytest ./tests -svvvv`

**Build & Publish Job**:
- Runs only on tags (e.g., `v1.1.9`)
- Builds sdist + wheel: `uv build`
- Publishes to PyPI: `uv publish`

## Common Coding Patterns

### Defining a Processor

```python
from sqlalchemy.orm import Session
import bq

app = bq.BeanQueue()

@app.processor(channel="images", retry_policy=bq.DelayRetry(delay=timedelta(seconds=120)))
def resize_image(db: Session, task: bq.Task, width: int, height: int):
    """
    Optional params:
    - db: Session - Database session
    - task: bq.Task - Current task object
    - savepoint: - Database savepoint for rollback
    """
    # Process task
    image = db.query(Image).filter(Image.task == task).one()
    image_utils.resize(image, size=(width, height))
    db.add(image)
    # auto_complete=True (default) commits changes automatically
```

### Submitting a Task

**Method 1: Direct model creation**
```python
db = Session()
task = bq.Task(
    channel="images",
    module="my_pkgs.processors",
    func_name="resize_image",
    kwargs={"width": 200, "height": 300},
)
db.add(task)
db.commit()  # Triggers NOTIFY
```

**Method 2: Via ProcessorHelper**
```python
from .processors import resize_image

db = Session()
task = resize_image.run(width=200, height=300)  # Creates Task object
image = Image(task=task, blob_name="...")
db.add(image)
db.add(task)
db.commit()  # Atomic transaction + NOTIFY
```

### Retry Policies

```python
# Fixed delay retry
delay_retry = bq.DelayRetry(delay=timedelta(seconds=120))

# Exponential backoff retry
exp_retry = bq.ExponentialBackoffRetry(base=2, exponent=3)

# Limit retry attempts
capped_retry = bq.LimitAttempt(max_attempts=3, retry_policy=delay_retry)

# Retry only specific exceptions
@app.processor(
    channel="images",
    retry_policy=delay_retry,
    retry_exceptions=ValueError,  # or (ValueError, TypeError)
)
def my_processor(...):
    pass
```

### Custom Models (Using Mixins)

```python
import uuid
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
import bq
from bq.models.task import listen_events
from .base import Base

class Task(bq.TaskModelMixin, Base):
    __tablename__ = "task"

    worker_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("worker.id", onupdate="CASCADE"),
        nullable=True,
        index=True,
    )
    worker: Mapped["Worker"] = relationship("Worker", back_populates="tasks")

# CRITICAL: Register for NOTIFY events
listen_events(Task)
```

### Running Workers Programmatically

```python
app = bq.BeanQueue(config=config)
app.process_tasks(channels=("images", "files"))
```

## Important Gotchas & Best Practices

### 1. **Always Register Custom Task Models with `listen_events()`**

If you define a custom Task model, you MUST call `bq.models.task.listen_events(YourTaskModel)` immediately after the class definition. This registers SQLAlchemy event listeners that trigger `NOTIFY` statements when tasks are inserted/updated.

**Bad**:
```python
class Task(bq.TaskModelMixin, Base):
    __tablename__ = "task"
    # ... fields ...
# Missing listen_events() - NOTIFY won't work!
```

**Good**:
```python
from bq.models.task import listen_events

class Task(bq.TaskModelMixin, Base):
    __tablename__ = "task"
    # ... fields ...

listen_events(Task)  # ✅ Correct
```

### 2. **Task Scheduling Accuracy**

Scheduled tasks (`task.scheduled_at`) are **not precise**. Workers poll on timeout (default 60s), so execution may be delayed by up to `POLL_TIMEOUT` seconds. For time-sensitive tasks, reduce `BQ_POLL_TIMEOUT`.

### 3. **Processor Package Discovery**

Processors are discovered via venusian scanning. Ensure:
- `BQ_PROCESSOR_PACKAGES` includes the root package containing `@app.processor` decorated functions
- Processors are imported at module level (not inside functions)
- Package `__init__.py` files exist for proper module discovery

### 4. **Database Connection Pooling**

BeanQueue uses `SingletonThreadPool` by default (suitable for single-threaded apps). For production, consider custom pooling:

```python
from sqlalchemy.pool import QueuePool

engine = create_engine(
    config.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
)
app = bq.BeanQueue(config=config, engine=engine)
```

### 5. **Worker Heartbeat & Dead Worker Detection**

- Workers update `last_heartbeat` every 30s (configurable via `BQ_WORKER_HEARTBEAT_PERIOD`)
- Workers are considered dead if no heartbeat for 100s (configurable via `BQ_WORKER_HEARTBEAT_TIMEOUT`)
- Dead workers' tasks are automatically rescheduled to `PENDING`

**Implication**: If a worker crashes, tasks may be delayed by up to `WORKER_HEARTBEAT_TIMEOUT` seconds before being reassigned.

### 6. **Transaction Boundaries in Processors**

With `auto_complete=True` (default), the processor's database session is committed automatically on success. If you need manual control:

```python
@app.processor(channel="images", auto_complete=False)
def my_processor(db: Session, ...):
    # Manual commit required
    db.commit()
```

### 7. **Subtask Parent Tracking**

Subtasks created within a processor automatically inherit the `parent_id`:

```python
@app.processor(channel="parent")
def parent_task(db: Session):
    subtask = child_task.run(param=value)
    db.add(subtask)  # subtask.parent_id automatically set to current task
    db.commit()
```

This works via `current_task` context variable (thread-safe).

### 8. **SKIP LOCKED Behavior**

Multiple workers can safely poll the same channel simultaneously. PostgreSQL's `SKIP LOCKED` ensures:
- Each task is locked by only one worker
- No duplicate processing
- High concurrency without contention

### 9. **Error Handling & Events**

Failed tasks generate Event records:
- `FAILED` - Task failed, no retry scheduled
- `FAILED_RETRY_SCHEDULED` - Task failed, retry scheduled
- `COMPLETE` - Task completed successfully (if event logging enabled)

Use blinker signals (`task_failure`, `worker_init`) for custom error handling.

### 10. **Metrics HTTP Server**

By default, BeanQueue starts an HTTP server on port 8000 with `/healthz` endpoint. Disable if running multiple workers on same host:

```bash
BQ_METRICS_HTTP_SERVER_ENABLED=False bq process images
```

## Key Files & Their Purposes

| File | Purpose | Key Functions/Classes |
|------|---------|----------------------|
| `bq/app.py` | Core BeanQueue class | `BeanQueue`, `process_tasks`, `processor` decorator |
| `bq/config.py` | Configuration management | `Config` (pydantic-settings) |
| `bq/models/task.py` | Task model + NOTIFY events | `Task`, `listen_events`, SQLAlchemy event handlers |
| `bq/models/worker.py` | Worker model | `Worker` |
| `bq/models/event.py` | Event model | `Event` |
| `bq/processors/processor.py` | Processor class | `Processor`, `ProcessorHelper`, task execution logic |
| `bq/processors/registry.py` | Processor registry | `Registry`, `collect` |
| `bq/processors/retry_policies.py` | Retry strategies | `DelayRetry`, `ExponentialBackoffRetry`, `LimitAttempt` |
| `bq/services/worker.py` | Worker service layer | `WorkerService`, heartbeat management |
| `bq/services/dispatch.py` | Task dispatch service | `DispatchService`, SKIP LOCKED queries, NOTIFY/LISTEN |
| `bq/cmds/process.py` | CLI process command | `process_cmd` |
| `bq/cmds/submit.py` | CLI submit command | `submit_cmd` |
| `bq/cmds/create_tables.py` | CLI create_tables command | `create_tables_cmd` |

## Git & Deployment Workflow

### Current Branch

**Feature Branch**: `claude/claude-md-mibn4z97syefxl2g-01XAxkDaP6gT36kV17gDTWWn`

**Important**: All development should happen on the designated feature branch. Push to this branch when work is complete.

### Git Operations

**Pushing Changes**:
```bash
git add .
git commit -m "Description of changes"
git push -u origin claude/claude-md-mibn4z97syefxl2g-01XAxkDaP6gT36kV17gDTWWn
```

**Important**: Branch names must start with `claude/` and match the session ID for authentication. If push fails with 403, verify branch name matches the session.

**Retry on Network Errors**: Retry up to 4 times with exponential backoff (2s, 4s, 8s, 16s).

### Release Process

1. Update version in `pyproject.toml`
2. Commit changes: `git commit -m "Bump version to X.Y.Z"`
3. Create tag: `git tag vX.Y.Z`
4. Push tag: `git push origin vX.Y.Z`
5. CircleCI automatically builds and publishes to PyPI

### Recent Commits

```
572e393 - Bump ver
2a753da - Fix passing to the wrong format param
4e9a0c6 - Revert "Add log format"
15eac70 - Bump ver
ff47c5b - Add log format
```

## Common Tasks for AI Assistants

### Adding a New Processor

1. Define processor function in a module under `PROCESSOR_PACKAGES`
2. Decorate with `@app.processor(channel="...")`
3. Optionally specify retry policy, auto_complete, retry_exceptions
4. Test with `bq submit <channel> <module> <func_name> -k '{...}'`

### Adding a New Model Field

1. Update model class (e.g., `bq/models/task.py`)
2. Create Alembic migration (if applicable)
3. Update factories in `tests/factories.py`
4. Update tests to cover new field

### Modifying Service Logic

1. Update service class (e.g., `bq/services/dispatch.py`)
2. Update corresponding tests in `tests/unit/services/`
3. Ensure acceptance tests still pass

### Adding Configuration Options

1. Add field to `Config` class in `bq/config.py`
2. Document in README.md
3. Add test in `tests/unit/test_config.py`

### Debugging Task Processing Issues

1. Enable debug logging: `bq -l debug process <channel>`
2. Check worker heartbeat: Query `bq_workers` table for `last_heartbeat`
3. Check task state: Query `bq_tasks` table for `state`, `error_message`
4. Check events: Query `bq_events` table for failure details
5. Verify NOTIFY/LISTEN: `LISTEN <channel>` in psql, then insert task

## Technology Stack Summary

| Component | Technology | Version |
|-----------|-----------|---------|
| Language | Python | 3.11+ |
| Package Manager | uv | latest |
| ORM | SQLAlchemy | 2.0.30+ |
| Database | PostgreSQL | 16.x |
| CLI | Click | 8.1.7+ |
| Config | pydantic-settings | 2.2.1+ |
| Events | blinker | 1.8.2+ |
| Decorator Discovery | venusian | 3.1.0+ |
| Logging UI | rich | 13.7.1+ |
| Testing | pytest + pytest-factoryboy | latest |
| Code Formatter | ruff | 0.11.6 |
| Import Sorter | reorder-python-imports | 3.10.0 |
| Build System | hatchling | latest |
| CI/CD | CircleCI | 2.1 |

## Useful SQL Queries for Debugging

```sql
-- Check pending tasks
SELECT id, channel, module, func_name, created_at, scheduled_at
FROM bq_tasks
WHERE state = 'PENDING'
ORDER BY created_at DESC;

-- Check failed tasks
SELECT id, channel, func_name, error_message, created_at
FROM bq_tasks
WHERE state = 'FAILED'
ORDER BY created_at DESC;

-- Check worker status
SELECT id, name, state, channels, last_heartbeat, created_at
FROM bq_workers
ORDER BY last_heartbeat DESC;

-- Check task events
SELECT e.id, e.type, e.error_message, e.created_at, t.channel, t.func_name
FROM bq_events e
JOIN bq_tasks t ON e.task_id = t.id
ORDER BY e.created_at DESC
LIMIT 20;

-- Find tasks stuck in PROCESSING
SELECT id, channel, func_name, worker_id, created_at
FROM bq_tasks
WHERE state = 'PROCESSING'
  AND created_at < NOW() - INTERVAL '10 minutes'
ORDER BY created_at;

-- Check channel activity
SELECT channel, state, COUNT(*) as count
FROM bq_tasks
GROUP BY channel, state
ORDER BY channel, state;
```

## External Resources

- **Repository**: https://github.com/LaunchPlatform/bq
- **CircleCI**: https://dl.circleci.com/status-badge/redirect/gh/LaunchPlatform/bq/tree/master
- **PyPI**: https://pypi.org/project/beanqueue/
- **Sponsor**: https://beanhub.io

## Alternative Projects

Similar PostgreSQL-based task queues for reference:
- solid_queue (Ruby/Rails)
- good_job (Ruby/Rails)
- graphile-worker (Node.js)
- postgres-tq (Go)
- pq (Python)
- PgQueuer (Python)
- procrastinate (Python)
- hatchet (Distributed workflows)

---

## Quick Reference

### Essential Commands

```bash
# Setup
bq create_tables

# Process tasks
bq process <channel>

# Submit test task
bq submit <channel> <module> <func> -k '{...}'

# Run tests
uv run python -m pytest ./tests -svvvv

# Format code
pre-commit run --all-files
```

### Essential Imports

```python
import bq
from sqlalchemy.orm import Session

app = bq.BeanQueue()
config = bq.Config(...)
task = bq.Task(...)
processor = app.processor(channel="...")
retry = bq.DelayRetry(...)
```

### Configuration Priority

1. Programmatic `Config` object passed to `BeanQueue()`
2. Environment variables with `BQ_` prefix
3. Default values in `bq/config.py`

---

**Last Updated**: 2025-11-23
**Version**: 1.1.9
**Maintained By**: Fang-Pen Lin (fangpen@launchplatform.com)
