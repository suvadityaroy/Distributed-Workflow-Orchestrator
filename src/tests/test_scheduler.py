"""Tests for the scheduler."""

from orchestrator.dag import DAG, Task
from orchestrator.persistence import InMemoryPersistence
from orchestrator.scheduler import Scheduler


def _build_dag() -> DAG:
	return DAG(
		id="demo",
		name="Demo",
		tasks={
			"task_a": Task(id="task_a", name="A", command="echo A"),
			"task_b": Task(id="task_b", name="B", command="echo B", dependencies=["task_a"]),
		},
	)


def test_schedule_dag_enqueues_roots() -> None:
	persistence = InMemoryPersistence()
	scheduler = Scheduler(persistence)
	scheduler.schedule_dag(_build_dag(), run_id="run-1")

	first = persistence.pop_task_queue(timeout=0)
	assert first is not None
	assert first["task_id"] == "task_a"
	assert persistence.pop_task_queue(timeout=0) is None


def test_schedule_dag_all_independent_tasks_enqueued() -> None:
	dag = DAG(
		id="demo",
		name="Demo",
		tasks={
			"task_a": Task(id="task_a", name="A", command="echo A"),
			"task_b": Task(id="task_b", name="B", command="echo B"),
		},
	)
	persistence = InMemoryPersistence()
	Scheduler(persistence).schedule_dag(dag, run_id="run-2")

	tasks = {
		persistence.pop_task_queue(timeout=0)["task_id"],
		persistence.pop_task_queue(timeout=0)["task_id"],
	}
	assert tasks == {"task_a", "task_b"}
