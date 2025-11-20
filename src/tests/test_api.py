"""Tests for the FastAPI surface."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from orchestrator.api import app, get_persistence
from orchestrator.scheduler import Scheduler
from orchestrator.persistence import InMemoryPersistence


def _dag_payload() -> dict:
	return {
		"id": "sample",
		"name": "Sample DAG",
		"tasks": {
			"task_a": {
				"id": "task_a",
				"name": "Task A",
				"command": "echo A",
			},
			"task_b": {
				"id": "task_b",
				"name": "Task B",
				"command": "echo B",
				"dependencies": ["task_a"],
			},
		},
	}


@pytest.fixture()
def client() -> Iterator[TestClient]:
	persistence = InMemoryPersistence()
	app.state.persistence = persistence
	app.dependency_overrides[get_persistence] = lambda: persistence
	client = TestClient(app)
	yield client
	client.close()
	app.dependency_overrides.clear()


def test_post_dag_stores_definition(client: TestClient) -> None:
	response = client.post("/dags", json=_dag_payload())
	assert response.status_code == 200
	assert response.json()["dag_id"] == "sample"


def test_trigger_run_invokes_scheduler(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
	client.post("/dags", json=_dag_payload())

	called = {}

	def fake_schedule(self, dag, run_id):
		called["dag_id"] = dag.id
		called["run_id"] = run_id

	monkeypatch.setattr(Scheduler, "schedule_dag", fake_schedule)

	response = client.post("/dags/sample/run")
	assert response.status_code == 200
	assert response.json()["run_id"]
	assert called["dag_id"] == "sample"
