# Distributed Workflow Orchestrator

🚀 **Live Demo:** [https://distributed-workflow-orchestrator-production.up.railway.app](https://distributed-workflow-orchestrator-production.up.railway.app)

Distributed Workflow Orchestrator is a FastAPI + Redis reference implementation for defining Directed Acyclic Graphs (DAGs) and executing them via pluggable workers. It emphasizes typed models, modular persistence, and production-focused ergonomics (Docker, CI, extensive tests) so teams can bootstrap resilient workflow execution quickly.

## Architecture
```
				+-------------+        +-------------+        +-----------------+
				|   Clients   |----->  |   FastAPI   |----->  |    Scheduler    |
				+-------------+        +-------------+        +-----------------+
								 |                      |                      |
								 |                      v                      v
								 |              +---------------+        +-------------+
								 +------------> | Persistence   | <----> |   Workers   |
																| (Redis/InMem) |        +-------------+
																+---------------+                |
																				 ^                        v
																				 |                 +-------------+
																				 +----------------- |  Executor  |
																														+-------------+
```

## Tech Stack
- `FastAPI` + `uvicorn` for the orchestration API with interactive dashboard UI
- `pydantic` DAG + payload models with strict validation
- Redis-backed persistence with in-memory fallback for tests
- File upload support with `python-multipart` and `PyYAML` for JSON/YAML parsing
- Modular scheduler/worker/executor components
- `pytest` + `httpx` for test coverage; GitHub Actions CI + Docker packaging

## Quick Start
1. Install Docker & Docker Compose.
2. Clone this repo and navigate to `orchestrator/`.
3. Launch the stack:
   ```bash
   docker-compose up --build
   ```
4. API becomes available at `http://localhost:8000` (docs at `/docs`).

## Running Tests Locally
```bash
python -m venv .venv && .\.venv\Scripts\activate   # Windows example
pip install -r requirements.txt
pytest
```

## Features
- 🎯 **Interactive Dashboard** - Visual UI at `/` with real-time metrics, DAG list, and run monitoring
- 📤 **Drag & Drop Upload** - Upload DAG files (JSON/YAML) directly in the browser
- 📊 **Live Metrics** - System statistics, task status breakdown, queue depth
- ▶️ **One-Click Execution** - Trigger workflow runs from the dashboard
- ✖️ **Run Cancellation** - Cancel running workflows on demand
- 🔄 **Auto-Refresh** - Dashboard updates every 5 seconds automatically

## API Examples
### Register a DAG (JSON)
```bash
curl -X POST http://localhost:8000/dags \
     -H "Content-Type: application/json" \
     -d '{"id":"sample","name":"Demo","tasks":{"task_a":{"id":"task_a","name":"Task A","command":"echo A"}}}'
```

### Upload a DAG File (JSON/YAML)
```bash
curl -X POST http://localhost:8000/dags/upload \
     -F "file=@examples/sample_upload.yaml"
```

### List All DAGs
```bash
curl http://localhost:8000/dags
```

### Trigger a Run
```bash
curl -X POST http://localhost:8000/dags/sample/run
```

### Inspect Run Status
```bash
curl http://localhost:8000/runs/<run_id>
```

### Get System Metrics
```bash
curl http://localhost:8000/metrics
```

### Cancel a Run
```bash
curl -X POST http://localhost:8000/runs/<run_id>/cancel
```

## Demo
```bash
# Upload DAG JSON
curl -X POST http://localhost:8000/dags \
     -H "Content-Type: application/json" \
     -d '{"id":"sample","name":"Demo","tasks":{"task_a":{"id":"task_a","name":"Task A","command":"echo A"}}}'

# Trigger the run
curl -X POST http://localhost:8000/dags/sample/run
```

## Sample DAG YAML
```yaml
id: sample
name: Sample DAG
tasks:
  task_a:
    id: task_a
    name: Echo A
    command: echo "A"
    retries: 1
    retry_delay_seconds: 3
    timeout_seconds: 30
  task_b:
    id: task_b
    name: Callable B
    callable: examples.sample_module:do_b
    dependencies: [task_a]
    retries: 2
    metadata:
      message: "hello"
  task_c:
    id: task_c
    name: Echo C
    command: sleep 1 && echo "C"
    dependencies: [task_a]
    timeout_seconds: 60
  task_d:
    id: task_d
    name: Done
    command: echo "done"
    dependencies: [task_b, task_c]
    retries: 0
```

## Design Decisions & Future Improvements
- Deterministic DAG validation with cycle detection prevents runaway workflows.
- Persistence interface cleanly abstracts Redis vs. in-memory usage, enabling fast tests and production reliability.
- Workers enforce structured retries with exponential backoff; metadata captured for auditability.
- Executors support shell and Python callables (sync or async) with strict timeout handling.
- Future: add distributed locking for multi-worker deployments, richer run metadata endpoints, and pluggable authentication/authorization on the API.
