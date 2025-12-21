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
		
		@keyframes gradient-shift {
			0% { background-position: 0% 50%; }
			50% { background-position: 100% 50%; }
			100% { background-position: 0% 50%; }
		}
		
		@keyframes fadeInUp {
			from {
				opacity: 0;
				transform: translateY(30px);
			}
			to {
				opacity: 1;
				transform: translateY(0);
			}
		}
		
		@keyframes slideInLeft {
			from {
				opacity: 0;
				transform: translateX(-30px);
			}
			to {
				opacity: 1;
				transform: translateX(0);
			}
		}
		
		@keyframes pulse {
			0%, 100% { transform: scale(1); }
			50% { transform: scale(1.05); }
		}
		
		@keyframes shimmer {
			0% { background-position: -1000px 0; }
			100% { background-position: 1000px 0; }
		}
		
		@keyframes bounce-in {
			0% { transform: scale(0.3); opacity: 0; }
			50% { transform: scale(1.05); }
			70% { transform: scale(0.9); }
			100% { transform: scale(1); opacity: 1; }
		}
		
		@keyframes rotate {
			from { transform: rotate(0deg); }
			to { transform: rotate(360deg); }
		}
		
		body { 
			font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
			background: linear-gradient(-45deg, #667eea, #764ba2, #f093fb, #4facfe);
			background-size: 400% 400%;
			animation: gradient-shift 15s ease infinite;
			min-height: 100vh;
			padding: 20px;
			overflow-x: hidden;
		}
		.container { 
			max-width: 1400px; 
			margin: 0 auto;
			animation: fadeInUp 0.6s ease-out;
		}
		
		.header {
			background: rgba(255, 255, 255, 0.95);
			backdrop-filter: blur(10px);
			border-radius: 16px;
			padding: 40px;
			margin-bottom: 30px;
			box-shadow: 0 20px 60px rgba(0,0,0,0.2);
			animation: slideInLeft 0.8s ease-out;
			border: 1px solid rgba(255,255,255,0.3);
		}
		h1 { 
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			-webkit-background-clip: text;
			-webkit-text-fill-color: transparent;
			background-clip: text;
			font-size: 3em; 
			margin-bottom: 12px;
			font-weight: 800;
			letter-spacing: -1px;
		}
		
		.subtitle { 
			color: #666; 
			font-size: 1.2em;
			font-weight: 300;
		}
		.grid { 
			display: grid; 
			grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); 
			gap: 25px; 
			margin-bottom: 20px; 
		}
		
		.card {
			background: rgba(255, 255, 255, 0.95);
			backdrop-filter: blur(10px);
			border-radius: 16px;
			padding: 28px;
			box-shadow: 0 10px 30px rgba(0,0,0,0.15);
			transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
			animation: bounce-in 0.6s ease-out;
			animation-fill-mode: both;
			border: 1px solid rgba(255,255,255,0.3);
			position: relative;
			overflow: hidden;
		}
		
		.card::before {
			content: '';
			position: absolute;
			top: 0;
			left: -100%;
			width: 100%;
			height: 100%;
			background: linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent);
			transition: left 0.5s;
		}
		
		.card:hover::before {
			left: 100%;
		}
		
		.card:hover { 
			transform: translateY(-8px) scale(1.02);
			box-shadow: 0 20px 40px rgba(0,0,0,0.2);
		}
		
		.card:nth-child(1) { animation-delay: 0.1s; }
		.card:nth-child(2) { animation-delay: 0.2s; }
		.card:nth-child(3) { animation-delay: 0.3s; }
		.card h3 { 
			color: #333; 
			margin-bottom: 20px; 
			font-size: 1.4em;
			font-weight: 700;
			letter-spacing: -0.5px;
		}
		
		.metric { 
			font-size: 3em; 
			font-weight: 800; 
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			-webkit-background-clip: text;
			-webkit-text-fill-color: transparent;
			background-clip: text;
			margin: 15px 0;
			animation: pulse 2s ease-in-out infinite;
		}
		.status-badge {
			display: inline-block;
			padding: 6px 14px;
			border-radius: 25px;
			font-size: 0.85em;
			font-weight: 700;
			margin: 3px;
			transition: all 0.2s;
			animation: fadeInUp 0.4s ease-out;
			text-transform: uppercase;
			letter-spacing: 0.5px;
		}
		
		.status-badge:hover {
			transform: translateY(-2px);
			box-shadow: 0 4px 8px rgba(0,0,0,0.2);
		}
		
		.status-success { 
			background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
			color: #155724; 
		}
		.status-running { 
			background: linear-gradient(135deg, #a1c4fd 0%, #c2e9fb 100%);
			color: #004085; 
		}
		.status-failed { 
			background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
			color: #721c24; 
		}
		.status-queued { 
			background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);
			color: #856404; 
		}
		.status-cancelled { 
			background: linear-gradient(135deg, #d7d2cc 0%, #304352 100%);
			color: #fff; 
		}
		.run-item, .dag-item {
			padding: 16px;
			border-bottom: 1px solid rgba(0,0,0,0.05);
			display: flex;
			justify-content: space-between;
			align-items: center;
			transition: all 0.2s;
			animation: slideInLeft 0.4s ease-out;
		}
		
		.run-item:hover, .dag-item:hover {
			background: rgba(102,126,234,0.05);
			padding-left: 20px;
		}
		
		.run-item:last-child, .dag-item:last-child { border-bottom: none; }
		
		.run-id { 
			font-family: 'SF Mono', Monaco, monospace; 
			font-size: 0.9em; 
			color: #666;
			background: rgba(0,0,0,0.05);
			padding: 4px 8px;
			border-radius: 4px;
		}
		.btn {
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			color: white;
			border: none;
			padding: 10px 20px;
			border-radius: 8px;
			cursor: pointer;
			font-size: 0.9em;
			font-weight: 600;
			transition: all 0.3s;
			box-shadow: 0 4px 15px rgba(102,126,234,0.4);
			text-transform: uppercase;
			letter-spacing: 0.5px;
		}
		
		.btn:hover { 
			transform: translateY(-2px);
			box-shadow: 0 6px 20px rgba(102,126,234,0.6);
		}
		
		.btn:active {
			transform: translateY(0);
		}
		
		.btn-danger { 
			background: linear-gradient(135deg, #ff6b6b 0%, #ee5a6f 100%);
			box-shadow: 0 4px 15px rgba(220,53,69,0.4);
		}
		
		.btn-danger:hover { 
			box-shadow: 0 6px 20px rgba(220,53,69,0.6);
		}
		.refresh-btn {
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			border: none;
			color: white;
			padding: 8px 20px;
			border-radius: 20px;
			cursor: pointer;
			font-weight: 600;
			box-shadow: 0 4px 15px rgba(102,126,234,0.4);
			transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
			font-size: 0.85em;
			text-transform: uppercase;
			letter-spacing: 0.5px;
		}
		
		.refresh-btn:hover {
			transform: translateY(-2px) scale(1.05);
			box-shadow: 0 6px 20px rgba(102,126,234,0.6);
		}
		
		.refresh-btn:active {
			transform: translateY(0) scale(1.02);
		}
		.empty-state { 
			color: #999; 
			font-style: italic; 
			padding: 30px; 
			text-align: center;
			font-size: 1.1em;
		}
		
		.timestamp { 
			color: #999; 
			font-size: 0.85em; 
			margin-top: 12px;
			font-weight: 500;
		}
		.upload-zone {
			border: 3px dashed rgba(102,126,234,0.5);
			border-radius: 16px;
			padding: 50px;
			text-align: center;
			cursor: pointer;
			transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
			background: linear-gradient(135deg, rgba(102,126,234,0.05) 0%, rgba(118,75,162,0.05) 100%);
			position: relative;
			overflow: hidden;
		}
		
		.upload-zone::before {
			content: '';
			position: absolute;
			top: 0;
			left: -100%;
			width: 100%;
			height: 100%;
			background: linear-gradient(90deg, transparent, rgba(255,255,255,0.3), transparent);
			transition: left 0.6s;
		}
		
		.upload-zone:hover::before {
			left: 100%;
		}
		
		.upload-zone:hover {
			border-color: #667eea;
			background: linear-gradient(135deg, rgba(102,126,234,0.15) 0%, rgba(118,75,162,0.15) 100%);
			transform: scale(1.02);
			box-shadow: 0 10px 30px rgba(102,126,234,0.2);
		}
		
		.upload-zone.dragover {
			border-color: #764ba2;
			background: linear-gradient(135deg, rgba(102,126,234,0.25) 0%, rgba(118,75,162,0.25) 100%);
			transform: scale(1.05);
			box-shadow: 0 15px 40px rgba(102,126,234,0.3);
		}
		.upload-icon { 
			font-size: 4em; 
			margin-bottom: 20px;
			animation: bounce-in 0.8s ease-out;
		}
		
		.upload-text { 
			color: #333; 
			font-size: 1.2em; 
			margin-bottom: 12px;
			font-weight: 600;
		}
		
		.upload-hint { 
			color: #666; 
			font-size: 0.95em;
			font-weight: 400;
		}
		
		.file-input { display: none; }
		
		.upload-status {
			margin-top: 20px;
			padding: 12px 20px;
			border-radius: 8px;
			display: none;
			font-weight: 600;
			animation: slideInLeft 0.4s ease-out;
		}
		
		.upload-status.success { 
			background: linear-gradient(135deg, #84fab0 0%, #8fd3f4 100%);
			color: #155724; 
			display: block;
		}
		
		.upload-status.error { 
			background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);
			color: #721c24; 
			display: block;
		}
		
		/* Loading animation */
		.loading {
			display: inline-block;
			width: 20px;
			height: 20px;
			border: 3px solid rgba(102,126,234,0.3);
			border-radius: 50%;
			border-top-color: #667eea;
			animation: rotate 1s linear infinite;
		}
		
		/* Professional Navigation */
		.navbar {
			position: sticky;
			top: 0;
			z-index: 999;
			background: rgba(255, 255, 255, 0.98);
			backdrop-filter: blur(20px);
			padding: 20px 0;
			margin-bottom: 30px;
			box-shadow: 0 4px 20px rgba(0,0,0,0.08);
			animation: slideInLeft 0.6s ease-out;
		}
		
		.navbar-content {
			max-width: 1400px;
			margin: 0 auto;
			padding: 0 20px;
			display: flex;
			justify-content: space-between;
			align-items: center;
		}
		
		.logo {
			display: flex;
			align-items: center;
			gap: 12px;
			font-size: 1.5em;
			font-weight: 800;
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			-webkit-background-clip: text;
			-webkit-text-fill-color: transparent;
			background-clip: text;
		}
		
		.nav-links {
			display: flex;
			gap: 30px;
			align-items: center;
		}
		
		.nav-link {
			color: #666;
			text-decoration: none;
			font-weight: 500;
			transition: all 0.2s;
			position: relative;
		}
		
		.nav-link:hover {
			color: #667eea;
		}
		
		.nav-link::after {
			content: '';
			position: absolute;
			bottom: -5px;
			left: 0;
			width: 0;
			height: 2px;
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			transition: width 0.3s;
		}
		
		.nav-link:hover::after {
			width: 100%;
		}
		
		/* Floating Particles */
		.particle {
			position: fixed;
			pointer-events: none;
			z-index: 1;
			opacity: 0.6;
		}
		
		@keyframes float {
			0%, 100% {
				transform: translateY(0) translateX(0) rotate(0deg);
				opacity: 0;
			}
			10% {
				opacity: 0.6;
			}
			90% {
				opacity: 0.3;
			}
			100% {
				transform: translateY(-100vh) translateX(50px) rotate(360deg);
				opacity: 0;
			}
		}
		
		/* Glow Effects */
		@keyframes glow {
			0%, 100% {
				box-shadow: 0 0 20px rgba(102,126,234,0.3), 0 0 40px rgba(102,126,234,0.2), 0 0 60px rgba(102,126,234,0.1);
			}
			50% {
				box-shadow: 0 0 30px rgba(102,126,234,0.5), 0 0 60px rgba(102,126,234,0.3), 0 0 90px rgba(102,126,234,0.2);
			}
		}
		
		.card.glow-effect {
			animation: bounce-in 0.6s ease-out, glow 3s ease-in-out infinite;
		}
		
		/* Ripple Effect */
		@keyframes ripple {
			0% {
				transform: scale(0);
				opacity: 1;
			}
			100% {
				transform: scale(4);
				opacity: 0;
			}
		}
		
		.ripple {
			position: absolute;
			border-radius: 50%;
			background: rgba(255, 255, 255, 0.6);
			animation: ripple 0.6s ease-out;
			pointer-events: none;
		}
		
		/* Stats Cards Enhancement */
		.stats-grid {
			display: grid;
			grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
			gap: 20px;
			margin-bottom: 30px;
		}
		
		.stat-card {
			background: rgba(255, 255, 255, 0.95);
			backdrop-filter: blur(10px);
			border-radius: 16px;
			padding: 24px;
			text-align: center;
			box-shadow: 0 10px 30px rgba(0,0,0,0.1);
			transition: all 0.3s;
			animation: bounce-in 0.6s ease-out;
			animation-fill-mode: both;
			border: 1px solid rgba(255,255,255,0.3);
		}
		
		.stat-card:hover {
			transform: translateY(-5px) scale(1.03);
			box-shadow: 0 15px 40px rgba(0,0,0,0.15);
		}
		
		.stat-icon {
			font-size: 2.5em;
			margin-bottom: 10px;
		}
		
		.stat-value {
			font-size: 2.5em;
			font-weight: 800;
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			-webkit-background-clip: text;
			-webkit-text-fill-color: transparent;
			background-clip: text;
			margin: 10px 0;
		}
		
		.stat-label {
			color: #666;
			font-size: 0.95em;
			font-weight: 500;
			text-transform: uppercase;
			letter-spacing: 1px;
		}
		
		/* Footer */
		.footer {
			margin-top: 60px;
			padding: 40px 0;
			text-align: center;
			background: rgba(255, 255, 255, 0.95);
			backdrop-filter: blur(10px);
			border-radius: 16px;
			box-shadow: 0 10px 30px rgba(0,0,0,0.1);
			animation: fadeInUp 0.8s ease-out;
		}
		
		.footer-content {
			color: #666;
			font-size: 0.9em;
		}
		
		.footer-links {
			display: flex;
			justify-content: center;
			gap: 30px;
			margin-top: 20px;
		}
		
		.footer-link {
			color: #667eea;
			text-decoration: none;
			font-weight: 600;
			transition: all 0.2s;
		}
		
		.footer-link:hover {
			color: #764ba2;
			transform: translateY(-2px);
		}
		
		/* Responsive Design */
		@media (max-width: 768px) {
			.navbar-content {
				flex-direction: column;
				gap: 15px;
			}
			
			.nav-links {
				flex-direction: column;
				gap: 15px;
			}
			
			h1 {
				font-size: 2em;
			}
			
			.grid {
				grid-template-columns: 1fr;
			}
			
			.stats-grid {
				grid-template-columns: repeat(2, 1fr);
			}
			
			.refresh-btn {
				bottom: 20px;
				right: 20px;
				padding: 12px 24px;
				font-size: 0.9em;
			}
		}
		
		/* Tooltip */
		[data-tooltip] {
			position: relative;
			cursor: help;
		}
		
		[data-tooltip]:hover::before {
			content: attr(data-tooltip);
			position: absolute;
			bottom: 100%;
			left: 50%;
			transform: translateX(-50%);
			padding: 8px 12px;
			background: rgba(0,0,0,0.9);
			color: white;
			border-radius: 6px;
			font-size: 0.85em;
			white-space: nowrap;
			z-index: 1000;
			animation: fadeInUp 0.2s ease-out;
		}
		
		/* Section Headers */
		.section-header {
			display: flex;
			justify-content: space-between;
			align-items: center;
			margin-bottom: 20px;
		}
		
		.section-title {
			font-size: 1.5em;
			font-weight: 700;
			color: #333;
		}
		
		.section-badge {
			padding: 4px 12px;
			background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
			color: white;
			border-radius: 20px;
			font-size: 0.75em;
			font-weight: 700;
		}
	</style>
</head>
<body>
	<!-- Navigation Bar -->
	<nav class="navbar">
		<div class="navbar-content">
			<div class="logo">
				<span>🚀</span>
				<span>Workflow Orchestrator</span>
			</div>
			<div class="nav-links">
				<a href="#metrics" class="nav-link">Metrics</a>
				<a href="#dags" class="nav-link">DAGs</a>
				<a href="#runs" class="nav-link">Runs</a>
			</div>
		</div>
	</nav>
	
	<div class="container">
		<!-- Hero Header -->
		<div class="header">
			<h1>🚀 Workflow Orchestrator Dashboard</h1>
			<p class="subtitle">Monitor DAGs, runs, and task execution in real-time with powerful analytics</p>
		</div>
		
		<!-- Quick Stats -->
		<div class="stats-grid" id="quickStats">
			<div class="stat-card" data-tooltip="Total number of registered DAGs">
				<div class="stat-icon">📁</div>
				<div class="stat-value" id="statDags">0</div>
				<div class="stat-label">DAGs</div>
			</div>
			<div class="stat-card" data-tooltip="Total workflow runs">
				<div class="stat-icon">▶️</div>
				<div class="stat-value" id="statRuns">0</div>
				<div class="stat-label">Runs</div>
			</div>
			<div class="stat-card" data-tooltip="Tasks currently in queue">
				<div class="stat-icon">⏳</div>
				<div class="stat-value" id="statQueue">0</div>
				<div class="stat-label">Queued</div>
			</div>
			<div class="stat-card" data-tooltip="Successfully completed tasks">
				<div class="stat-icon">✅</div>
				<div class="stat-value" id="statSuccess">0</div>
				<div class="stat-label">Success</div>
			</div>
		</div>
		
		<!-- Upload Section -->
		<div id="dags" class="card" style="margin-bottom: 30px;">
			<div class="section-header">
				<h3>📤 Upload DAG File</h3>
				<span class="section-badge">JSON • YAML</span>
			</div>
			<div class="upload-zone" id="uploadZone">
				<div class="upload-icon">📁</div>
				<div class="upload-text">Drop your DAG file here or click to browse</div>
				<div class="upload-hint">Supports JSON and YAML files • Max 10MB</div>
				<input type="file" id="fileInput" class="file-input" accept=".json,.yaml,.yml">
			</div>
			<div class="upload-status" id="uploadStatus"></div>
		</div>
		
		<!-- Main Content Grid -->
		<div class="grid">
			<div id="metrics" class="card">
				<div class="section-header">
					<h3>📊 System Metrics</h3>
				</div>
				<div id="metrics-content">Loading...</div>
			</div>
			
			<div class="card">
				<div class="section-header">
					<h3>📁 Registered DAGs</h3>
					<span class="section-badge" id="dagsCount">0</span>
				</div>
				<div id="dags-content">Loading...</div>
			</div>
			
			<div id="runs" class="card">
				<div class="section-header">
					<h3>▶️ Recent Runs</h3>
					<div style="display: flex; gap: 10px; align-items: center;">
						<span class="section-badge" id="runsCount">0</span>
						<button class="refresh-btn" onclick="loadAll()" data-tooltip="Refresh all data">🔄 Refresh</button>
					</div>
				</div>
				<div id="runs-content">Loading...</div>
			</div>
		</div>
	</div>
	
	<script>
		// Update quick stats
		function updateQuickStats(data) {
			document.getElementById('statDags').textContent = data.dags_registered || 0;
			document.getElementById('statRuns').textContent = data.runs_total || 0;
			document.getElementById('statQueue').textContent = data.queue_depth || 0;
			document.getElementById('statSuccess').textContent = data.tasks_by_status?.success || 0;
		}
		
		async function loadMetrics() {
			try {
				const res = await fetch('/metrics');
				const data = await res.json();
				
				// Update quick stats
				updateQuickStats(data);
				
				const statusHtml = Object.entries(data.tasks_by_status || {})
					.map(([status, count]) => `<span class="status-badge status-${status}">${status}: ${count}</span>`)
					.join('');
				document.getElementById('metrics-content').innerHTML = `
					<div class="metric">${data.dags_registered}</div>
					<div>Registered DAGs</div>
					<div class="metric" style="font-size: 1.8em; margin-top: 15px;">${data.runs_total}</div>
					<div>Total Runs</div>
					<div style="margin-top: 15px;">${statusHtml || '<span class="empty-state">No tasks yet</span>'}</div>
					<div class="timestamp">Queue: ${data.queue_depth} tasks • Last updated: ${new Date().toLocaleTimeString()}</div>
				`;
			} catch (e) {
				document.getElementById('metrics-content').innerHTML = '<div class="empty-state">Failed to load</div>';
			}
		}
		
		async function loadDAGs() {
			try {
				const res = await fetch('/dags');
				const data = await res.json();
				document.getElementById('dagsCount').textContent = data.dags.length;
				
				if (data.dags.length === 0) {
					document.getElementById('dags-content').innerHTML = '<div class="empty-state">No DAGs registered yet. Upload a DAG file to get started.</div>';
					return;
				}
				const html = data.dags.map((id, index) => `
					<div class="dag-item" style="animation-delay: ${index * 0.05}s;">
						<div>
							<strong>${id}</strong>
							<br>
							<span style="font-size: 0.85em; color: #999;">Ready to execute</span>
						</div>
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
				document.getElementById('runsCount').textContent = data.count || 0;
				
				if (data.runs.length === 0) {
					document.getElementById('runs-content').innerHTML = '<div class="empty-state">No runs yet. Trigger a DAG to see run history.</div>';
					return;
				}
				const html = data.runs.map((run, index) => `
					<div class="run-item" style="animation-delay: ${index * 0.05}s;">
						<div>
							<strong>${run.dag_id || 'Unknown DAG'}</strong><br>
							<span class="run-id">${run.run_id}</span>
						</div>
						<div style="display: flex; gap: 10px; align-items: center;">
							<span class="status-badge status-${run.status || 'unknown'}">${run.status || 'unknown'}</span>
							${run.status !== 'cancelled' && run.status !== 'completed' ? 
								`<button class="btn btn-danger" onclick="cancelRun('${run.run_id}')">✖ Cancel</button>` : ''}
						</div>
					</div>
				`).join('');
				document.getElementById('runs-content').innerHTML = html;
			} catch (e) {
				document.getElementById('runs-content').innerHTML = '<div class="empty-state">Failed to load</div>';
			}
		}
		
		async function triggerRun(dagId) {
			if (!confirm(`🚀 Trigger workflow run for DAG: ${dagId}?`)) return;
			try {
				const res = await fetch(`/dags/${dagId}/run`, { method: 'POST' });
				const data = await res.json();
				
				// Show success notification
				const notification = document.createElement('div');
				notification.className = 'upload-status success';
				notification.textContent = `✓ Run triggered! ID: ${data.run_id}`;
				notification.style.position = 'fixed';
				notification.style.top = '20px';
				notification.style.right = '20px';
				notification.style.zIndex = '9999';
				notification.style.display = 'block';
				document.body.appendChild(notification);
				
				setTimeout(() => notification.remove(), 3000);
				loadAll();
			} catch (e) {
				alert('❌ Failed to trigger run: ' + e.message);
			}
		}
		
		async function cancelRun(runId) {
			if (!confirm(`⚠️ Cancel workflow run: ${runId}?`)) return;
			try {
				await fetch(`/runs/${runId}/cancel`, { method: 'POST' });
				
				// Show success notification
				const notification = document.createElement('div');
				notification.className = 'upload-status';
				notification.style.background = 'linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%)';
				notification.style.color = '#856404';
				notification.textContent = '✓ Run cancelled successfully';
				notification.style.position = 'fixed';
				notification.style.top = '20px';
				notification.style.right = '20px';
				notification.style.zIndex = '9999';
				notification.style.display = 'block';
				document.body.appendChild(notification);
				
				setTimeout(() => notification.remove(), 3000);
				loadAll();
			} catch (e) {
				alert('❌ Failed to cancel run: ' + e.message);
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
			uploadStatus.textContent = '⏳ Uploading and validating...';
			uploadStatus.style.display = 'block';
			uploadStatus.style.background = 'linear-gradient(135deg, #a1c4fd 0%, #c2e9fb 100%)';
			uploadStatus.style.color = '#004085';
			
			try {
				const response = await fetch('/dags/upload', {
					method: 'POST',
					body: formData
				});
				
				const result = await response.json();
				
				if (response.ok) {
					uploadStatus.className = 'upload-status success';
					uploadStatus.textContent = `✓ Success! DAG "${result.dag_id}" uploaded from ${result.filename}`;
					fileInput.value = '';
					setTimeout(() => {
						loadAll();
						uploadStatus.style.display = 'none';
					}, 3000);
				} else {
					uploadStatus.className = 'upload-status error';
					uploadStatus.textContent = `✗ Upload failed: ${result.detail || 'Unknown error'}`;
				}
			} catch (error) {
				uploadStatus.className = 'upload-status error';
				uploadStatus.textContent = `✗ Network error: ${error.message}`;
			}
		}
		
		// Load on page load
		loadAll();
		
		// Auto-refresh every 5 seconds
		setInterval(loadAll, 5000);
		
		// Smooth scroll for anchor links
		document.querySelectorAll('a[href^="#"]').forEach(anchor => {
			anchor.addEventListener('click', function (e) {
				e.preventDefault();
				const target = document.querySelector(this.getAttribute('href'));
				if (target) {
					target.scrollIntoView({ behavior: 'smooth', block: 'start' });
				}
			});
		});
		
		// Create floating particles
		function createParticle() {
			const particle = document.createElement('div');
			particle.className = 'particle';
			particle.textContent = ['✨', '⭐', '💫', '🌟', '✦'][Math.floor(Math.random() * 5)];
			particle.style.left = Math.random() * 100 + '%';
			particle.style.fontSize = (Math.random() * 20 + 10) + 'px';
			particle.style.animation = `float ${Math.random() * 10 + 15}s linear`;
			document.body.appendChild(particle);
			
			setTimeout(() => particle.remove(), 25000);
		}
		
		// Generate particles periodically
		setInterval(createParticle, 3000);
		for (let i = 0; i < 5; i++) {
			setTimeout(createParticle, i * 600);
		}
		
		// Add ripple effect on button clicks
		document.addEventListener('click', function(e) {
			if (e.target.classList.contains('btn') || e.target.classList.contains('refresh-btn')) {
				const ripple = document.createElement('span');
				ripple.className = 'ripple';
				ripple.style.width = ripple.style.height = '10px';
				ripple.style.left = e.offsetX + 'px';
				ripple.style.top = e.offsetY + 'px';
				e.target.appendChild(ripple);
				setTimeout(() => ripple.remove(), 600);
			}
		});
		
		// Add glow effect to cards periodically
		setInterval(() => {
			const cards = document.querySelectorAll('.card');
			cards.forEach((card, index) => {
				setTimeout(() => {
					card.classList.add('glow-effect');
					setTimeout(() => card.classList.remove('glow-effect'), 3000);
				}, index * 500);
			});
		}, 15000);
	</script>
</body>
</html>
"""


@app.get("/health")
def health() -> dict:
	"""Basic health check."""

	return {"status": "ok"}
