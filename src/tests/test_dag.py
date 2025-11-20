"""Tests for DAG models."""

import pytest

from orchestrator.dag import DAG, Task, CycleError


def _simple_dag() -> DAG:
	return DAG(
		id="demo",
		name="Demo",
		tasks={
			"task_a": Task(id="task_a", name="A", command="echo A"),
			"task_b": Task(id="task_b", name="B", command="echo B", dependencies=["task_a"]),
			"task_c": Task(id="task_c", name="C", command="echo C", dependencies=["task_b"]),
		},
	)


def test_topological_sort_simple() -> None:
	dag = _simple_dag()
	assert dag.topological_sort() == ["task_a", "task_b", "task_c"]


def test_cycle_detection() -> None:
	dag = DAG(
		id="cycle",
		name="Cycle",
		tasks={
			"task_a": Task(id="task_a", name="A", command="echo A", dependencies=["task_c"]),
			"task_b": Task(id="task_b", name="B", command="echo B", dependencies=["task_a"]),
			"task_c": Task(id="task_c", name="C", command="echo C", dependencies=["task_b"]),
		},
	)
	with pytest.raises(CycleError):
		dag.validate()


def test_missing_dependency() -> None:
	dag = DAG(
		id="missing",
		name="Missing",
		tasks={
			"task_a": Task(id="task_a", name="A", command="echo A", dependencies=["task_x"]),
		},
	)
	with pytest.raises(ValueError, match="undefined dependencies"):
		dag.validate()
