"""FastAPI application entrypoint."""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List

from fastapi import Depends, FastAPI, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse
import json
import yaml

from .dag import DAG
from .persistence import PersistenceProtocol, get_persistence_from_env
from .scheduler import Scheduler
from .utils import setup_logging


setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
	"""Manage application lifespan."""
	app.state.persistence = get_persistence_from_env()
	yield


app = FastAPI(title="Distributed Workflow Orchestrator", version="1.0.0", lifespan=lifespan)


def get_persistence() -> PersistenceProtocol:
	persistence = getattr(app.state, "persistence", None)
	if persistence is None:
		persistence = get_persistence_from_env()
		app.state.persistence = persistence
	return persistence


@app.post("/dags")
def register_dag(dag: DAG, persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""Register or update a DAG definition."""

	dag.validate()
	persistence.save_dag(dag.id, dag.model_dump_json())
	return {"dag_id": dag.id}


@app.post("/dags/upload")
async def upload_dag(
	file: UploadFile = File(...),
	persistence: PersistenceProtocol = Depends(get_persistence)
) -> dict:
	"""Upload and register a DAG from JSON or YAML file."""
	
	content = await file.read()
	filename = file.filename.lower()
	
	try:
		# Parse based on file extension
		if filename.endswith('.json'):
			data = json.loads(content.decode('utf-8'))
		elif filename.endswith(('.yaml', '.yml')):
			data = yaml.safe_load(content.decode('utf-8'))
		else:
			raise HTTPException(
				status_code=400,
				detail="Unsupported file format. Please upload JSON or YAML files."
			)
		
		# Create DAG from parsed data
		dag = DAG(**data)
		dag.validate()
		persistence.save_dag(dag.id, dag.model_dump_json())
		
		return {"dag_id": dag.id, "filename": file.filename, "status": "uploaded"}
	
	except json.JSONDecodeError as e:
		raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")
	except yaml.YAMLError as e:
		raise HTTPException(status_code=400, detail=f"Invalid YAML: {str(e)}")
	except ValueError as e:
		raise HTTPException(status_code=400, detail=f"Invalid DAG: {str(e)}")
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.post("/dags/{dag_id}/run")
def trigger_run(dag_id: str, persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""Create a new run for a stored DAG."""

	dag_json = persistence.load_dag(dag_id)
	if not dag_json:
		raise HTTPException(status_code=404, detail="DAG not found")

	dag = DAG.model_validate_json(dag_json)
	run_id = str(uuid.uuid4())
	Scheduler(persistence).schedule_dag(dag, run_id)
	return {"run_id": run_id}


@app.get("/runs/{run_id}")
def get_run(run_id: str, persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""Return metadata and task statuses for a run."""

	run_meta = persistence.get_task_status(f"run:{run_id}")
	if not run_meta:
		raise HTTPException(status_code=404, detail="Run not found")

	task_ids: List[str] = run_meta.get("task_ids", [])
	tasks = []
	for task_id in task_ids:
		status = persistence.get_task_status(f"{run_id}:{task_id}")
		tasks.append({"task_id": task_id, **status} if status else {"task_id": task_id, "status": "pending"})

	return {"run_id": run_id, "metadata": run_meta, "tasks": tasks}


@app.get("/tasks/{task_run_id}")
def get_task(task_run_id: str, persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""Return detailed task execution logs."""

	status = persistence.get_task_status(task_run_id)
	if not status:
		raise HTTPException(status_code=404, detail="Task not found")
	return status


@app.get("/dags")
def list_dags(persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""List all registered DAGs."""
	# In-memory persistence stores DAGs; for Redis we'd scan keys
	if hasattr(persistence, "_dags"):
		dag_ids = list(persistence._dags.keys())
	else:
		# For Redis: scan pattern orchestrator:dag:*
		dag_ids = []
	return {"dags": dag_ids, "count": len(dag_ids)}


@app.get("/runs")
def list_runs(limit: int = 20, persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""List recent runs with status."""
	# Simplified: scan statuses starting with 'run:'
	if hasattr(persistence, "_statuses"):
		runs = [
			{"run_id": key.replace("run:", ""), **status}
			for key, status in persistence._statuses.items()
			if key.startswith("run:")
		][:limit]
	else:
		runs = []
	return {"runs": runs, "count": len(runs)}


@app.post("/runs/{run_id}/cancel")
def cancel_run(run_id: str, persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""Cancel a running workflow."""
	run_meta = persistence.get_task_status(f"run:{run_id}")
	if not run_meta:
		raise HTTPException(status_code=404, detail="Run not found")
	
	# Mark run as cancelled
	run_meta["status"] = "cancelled"
	persistence.save_task_status(f"run:{run_id}", "cancelled", run_meta)
	
	# Mark all queued/running tasks as cancelled
	task_ids: List[str] = run_meta.get("task_ids", [])
	for task_id in task_ids:
		status = persistence.get_task_status(f"{run_id}:{task_id}")
		if status and status.get("status") in ["queued", "running"]:
			status["status"] = "cancelled"
			persistence.save_task_status(f"{run_id}:{task_id}", "cancelled", status)
	
	return {"run_id": run_id, "status": "cancelled"}


@app.get("/tasks/{task_run_id}/retries")
def get_task_retries(task_run_id: str, persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""Get retry history for a task."""
	# Extract base task identifier
	parts = task_run_id.rsplit(":", 1)
	if len(parts) != 2:
		raise HTTPException(status_code=400, detail="Invalid task_run_id format")
	
	base_key = parts[0]
	retries = []
	
	# Scan for all attempts
	if hasattr(persistence, "_statuses"):
		for key, status in persistence._statuses.items():
			if key.startswith(base_key + ":"):
				retries.append({"task_run_id": key, **status})
	
	retries.sort(key=lambda x: x.get("task_run_id", ""))
	return {"task_run_id": task_run_id, "retries": retries, "count": len(retries)}


@app.get("/metrics")
def get_metrics(persistence: PersistenceProtocol = Depends(get_persistence)) -> dict:
	"""Get system metrics and statistics."""
	metrics: Dict[str, Any] = {
		"timestamp": time.time(),
		"dags_registered": 0,
		"runs_total": 0,
		"tasks_by_status": defaultdict(int),
		"queue_depth": 0,
	}
	
	if hasattr(persistence, "_dags"):
		metrics["dags_registered"] = len(persistence._dags)
	
	if hasattr(persistence, "_statuses"):
		for key, status in persistence._statuses.items():
			if key.startswith("run:"):
				metrics["runs_total"] += 1
			elif ":" in key and not key.startswith("run:"):
				task_status = status.get("status", "unknown")
				metrics["tasks_by_status"][task_status] += 1
	
	if hasattr(persistence, "_queue"):
		metrics["queue_depth"] = persistence._queue.qsize()
	
	metrics["tasks_by_status"] = dict(metrics["tasks_by_status"])
	return metrics


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
	"""Dashboard UI for monitoring workflows."""
	return """
<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>Workflow Orchestrator Dashboard</title>
	<style>
		* { margin: 0; padding: 0; box-sizing: border-box; }
		body { 
			font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			min-height: 100vh;
			padding: 20px;
		}
		.container { max-width: 1400px; margin: 0 auto; }
		.header {
			background: white;
			border-radius: 12px;
			padding: 30px;
			margin-bottom: 20px;
			box-shadow: 0 4px 6px rgba(0,0,0,0.1);
		}
		h1 { color: #667eea; font-size: 2.5em; margin-bottom: 10px; }
		.subtitle { color: #666; font-size: 1.1em; }
		.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
		.card {
			background: white;
			border-radius: 12px;
			padding: 24px;
			box-shadow: 0 4px 6px rgba(0,0,0,0.1);
			transition: transform 0.2s;
		}
		.card:hover { transform: translateY(-5px); box-shadow: 0 8px 12px rgba(0,0,0,0.15); }
		.card h3 { color: #333; margin-bottom: 15px; font-size: 1.3em; }
		.metric { font-size: 2.5em; font-weight: bold; color: #667eea; margin: 10px 0; }
		.status-badge {
			display: inline-block;
			padding: 4px 12px;
			border-radius: 20px;
			font-size: 0.85em;
			font-weight: 600;
			margin: 2px;
		}
		.status-success { background: #d4edda; color: #155724; }
		.status-running { background: #cce5ff; color: #004085; }
		.status-failed { background: #f8d7da; color: #721c24; }
		.status-queued { background: #fff3cd; color: #856404; }
		.status-cancelled { background: #e2e3e5; color: #383d41; }
		.run-item, .dag-item {
			padding: 12px;
			border-bottom: 1px solid #eee;
			display: flex;
			justify-content: space-between;
			align-items: center;
		}
		.run-item:last-child, .dag-item:last-child { border-bottom: none; }
		.run-id { font-family: monospace; font-size: 0.9em; color: #666; }
		.btn {
			background: #667eea;
			color: white;
			border: none;
			padding: 8px 16px;
			border-radius: 6px;
			cursor: pointer;
			font-size: 0.9em;
			transition: background 0.2s;
		}
		.btn:hover { background: #5568d3; }
		.btn-danger { background: #dc3545; }
		.btn-danger:hover { background: #c82333; }
		.refresh-btn {
			position: fixed;
			bottom: 30px;
			right: 30px;
			background: white;
			border: 2px solid #667eea;
			color: #667eea;
			padding: 12px 24px;
			border-radius: 50px;
			cursor: pointer;
			font-weight: 600;
			box-shadow: 0 4px 12px rgba(0,0,0,0.15);
			transition: all 0.2s;
		}
		.refresh-btn:hover {
			background: #667eea;
			color: white;
			transform: scale(1.05);
		}
		.empty-state { color: #999; font-style: italic; padding: 20px; text-align: center; }
		.timestamp { color: #999; font-size: 0.85em; margin-top: 10px; }
		.upload-zone {
			border: 3px dashed #667eea;
			border-radius: 12px;
			padding: 40px;
			text-align: center;
			cursor: pointer;
			transition: all 0.3s;
			background: rgba(255,255,255,0.05);
		}
		.upload-zone:hover, .upload-zone.dragover {
			border-color: #5568d3;
			background: rgba(102,126,234,0.1);
			transform: scale(1.02);
		}
		.upload-zone.dragover {
			background: rgba(102,126,234,0.2);
		}
		.upload-icon { font-size: 3em; margin-bottom: 15px; }
		.upload-text { color: #333; font-size: 1.1em; margin-bottom: 10px; }
		.upload-hint { color: #666; font-size: 0.9em; }
		.file-input { display: none; }
		.upload-status {
			margin-top: 15px;
			padding: 10px;
			border-radius: 6px;
			display: none;
		}
		.upload-status.success { background: #d4edda; color: #155724; display: block; }
		.upload-status.error { background: #f8d7da; color: #721c24; display: block; }
	</style>
</head>
<body>
	<div class="container">
		<div class="header">
			<h1>🚀 Workflow Orchestrator</h1>
			<p class="subtitle">Monitor DAGs, runs, and task execution in real-time</p>
		</div>
		
		<div class="card" style="margin-bottom: 20px;">
			<h3>📤 Upload DAG File</h3>
			<div class="upload-zone" id="uploadZone">
				<div class="upload-icon">📁</div>
				<div class="upload-text">Drop your DAG file here or click to browse</div>
				<div class="upload-hint">Supports JSON and YAML files</div>
				<input type="file" id="fileInput" class="file-input" accept=".json,.yaml,.yml">
			</div>
			<div class="upload-status" id="uploadStatus"></div>
		</div>
		
		<div class="grid">
			<div class="card">
				<h3>📊 Metrics</h3>
				<div id="metrics-content">Loading...</div>
			</div>
			
			<div class="card">
				<h3>📁 DAGs</h3>
				<div id="dags-content">Loading...</div>
			</div>
			
			<div class="card">
				<h3>▶️ Recent Runs</h3>
				<div id="runs-content">Loading...</div>
			</div>
		</div>
		
		<button class="refresh-btn" onclick="loadAll()">🔄 Refresh</button>
	</div>
	
	<script>
		async function loadMetrics() {
			try {
				const res = await fetch('/metrics');
				const data = await res.json();
				const statusHtml = Object.entries(data.tasks_by_status || {})
					.map(([status, count]) => `<span class="status-badge status-${status}">${status}: ${count}</span>`)
					.join('');
				document.getElementById('metrics-content').innerHTML = `
					<div class="metric">${data.dags_registered}</div>
					<div>Registered DAGs</div>
					<div class="metric" style="font-size: 1.8em; margin-top: 15px;">${data.runs_total}</div>
					<div>Total Runs</div>
					<div style="margin-top: 15px;">${statusHtml || '<span class="empty-state">No tasks yet</span>'}</div>
					<div class="timestamp">Queue: ${data.queue_depth} tasks</div>
				`;
			} catch (e) {
				document.getElementById('metrics-content').innerHTML = '<div class="empty-state">Failed to load</div>';
			}
		}
		
		async function loadDAGs() {
			try {
				const res = await fetch('/dags');
				const data = await res.json();
				if (data.dags.length === 0) {
					document.getElementById('dags-content').innerHTML = '<div class="empty-state">No DAGs registered</div>';
					return;
				}
				const html = data.dags.map(id => `
					<div class="dag-item">
						<strong>${id}</strong>
						<button class="btn" onclick="triggerRun('${id}')">▶️ Run</button>
					</div>
				`).join('');
				document.getElementById('dags-content').innerHTML = html;
			} catch (e) {
				document.getElementById('dags-content').innerHTML = '<div class="empty-state">Failed to load</div>';
			}
		}
		
		async function loadRuns() {
			try {
				const res = await fetch('/runs?limit=10');
				const data = await res.json();
				if (data.runs.length === 0) {
					document.getElementById('runs-content').innerHTML = '<div class="empty-state">No runs yet</div>';
					return;
				}
				const html = data.runs.map(run => `
					<div class="run-item">
						<div>
							<strong>${run.dag_id || 'Unknown'}</strong><br>
							<span class="run-id">${run.run_id}</span>
						</div>
						<div>
							<span class="status-badge status-${run.status || 'unknown'}">${run.status || 'unknown'}</span>
							${run.status !== 'cancelled' && run.status !== 'completed' ? 
								`<button class="btn btn-danger" onclick="cancelRun('${run.run_id}')">✖</button>` : ''}
						</div>
					</div>
				`).join('');
				document.getElementById('runs-content').innerHTML = html;
			} catch (e) {
				document.getElementById('runs-content').innerHTML = '<div class="empty-state">Failed to load</div>';
			}
		}
		
		async function triggerRun(dagId) {
			if (!confirm(`Trigger run for DAG: ${dagId}?`)) return;
			try {
				const res = await fetch(`/dags/${dagId}/run`, { method: 'POST' });
				const data = await res.json();
				alert(`Run triggered! ID: ${data.run_id}`);
				loadAll();
			} catch (e) {
				alert('Failed to trigger run: ' + e.message);
			}
		}
		
		async function cancelRun(runId) {
			if (!confirm(`Cancel run: ${runId}?`)) return;
			try {
				await fetch(`/runs/${runId}/cancel`, { method: 'POST' });
				alert('Run cancelled');
				loadAll();
			} catch (e) {
				alert('Failed to cancel run: ' + e.message);
			}
		}
		
		function loadAll() {
			loadMetrics();
			loadDAGs();
			loadRuns();
		}
		
		// File upload functionality
		const uploadZone = document.getElementById('uploadZone');
		const fileInput = document.getElementById('fileInput');
		const uploadStatus = document.getElementById('uploadStatus');
		
		// Click to browse
		uploadZone.addEventListener('click', () => fileInput.click());
		
		// File input change
		fileInput.addEventListener('change', (e) => {
			if (e.target.files.length > 0) {
				uploadFile(e.target.files[0]);
			}
		});
		
		// Drag and drop
		uploadZone.addEventListener('dragover', (e) => {
			e.preventDefault();
			uploadZone.classList.add('dragover');
		});
		
		uploadZone.addEventListener('dragleave', () => {
			uploadZone.classList.remove('dragover');
		});
		
		uploadZone.addEventListener('drop', (e) => {
			e.preventDefault();
			uploadZone.classList.remove('dragover');
			if (e.dataTransfer.files.length > 0) {
				uploadFile(e.dataTransfer.files[0]);
			}
		});
		
		async function uploadFile(file) {
			const formData = new FormData();
			formData.append('file', file);
			
			uploadStatus.className = 'upload-status';
			uploadStatus.textContent = 'Uploading...';
			uploadStatus.style.display = 'block';
			
			try {
				const response = await fetch('/dags/upload', {
					method: 'POST',
					body: formData
				});
				
				const result = await response.json();
				
				if (response.ok) {
					uploadStatus.className = 'upload-status success';
					uploadStatus.textContent = `✓ DAG "${result.dag_id}" uploaded successfully from ${result.filename}`;
					fileInput.value = '';
					setTimeout(() => {
						loadAll();
						uploadStatus.style.display = 'none';
					}, 3000);
				} else {
					uploadStatus.className = 'upload-status error';
					uploadStatus.textContent = `✗ Upload failed: ${result.detail}`;
				}
			} catch (error) {
				uploadStatus.className = 'upload-status error';
				uploadStatus.textContent = `✗ Upload failed: ${error.message}`;
			}
		}
		
		// Load on page load
		loadAll();
		
		// Auto-refresh every 5 seconds
		setInterval(loadAll, 5000);
	</script>
</body>
</html>
"""


@app.get("/health")
def health() -> dict:
	"""Basic health check."""

	return {"status": "ok"}
