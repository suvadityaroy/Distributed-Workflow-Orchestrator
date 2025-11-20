"""DAG and Task models along with validation helpers."""

from collections import deque
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class CycleError(ValueError):
	"""Raised when a DAG contains a circular dependency."""


class Task(BaseModel):
	"""Definition of an individual DAG task."""

	id: str
	name: str
	command: Optional[str] = None
	callable: Optional[str] = None
	retries: int = 0
	retry_delay_seconds: int = 5
	dependencies: List[str] = Field(default_factory=list)
	timeout_seconds: Optional[int] = None
	metadata: Dict[str, Any] = Field(default_factory=dict)

	@field_validator("retries", "retry_delay_seconds")
	@classmethod
	def _non_negative(cls, value: int) -> int:
		if value < 0:
			raise ValueError("Retries and retry delay must be non-negative")
		return value

	@model_validator(mode="after")
	def _ensure_command_or_callable(self) -> "Task":
		if not self.command and not self.callable:
			raise ValueError("Task must define either 'command' or 'callable'")
		for dep in self.dependencies:
			if dep == self.id:
				raise ValueError("Task cannot depend on itself")
		return self


class DAG(BaseModel):
	"""Directed acyclic graph describing task relationships."""

	id: str
	name: str
	tasks: Dict[str, Task]

	def validate(self) -> None:
		"""Validate dependencies and ensure the DAG is acyclic."""

		task_ids = set(self.tasks)
		for task in self.tasks.values():
			missing = set(task.dependencies) - task_ids
			if missing:
				raise ValueError(
					f"Task '{task.id}' references undefined dependencies: {sorted(missing)}"
				)

		if self._has_cycle():
			raise CycleError(f"DAG '{self.id}' contains a cycle")

	def _has_cycle(self) -> bool:
		indegree: Dict[str, int] = {task_id: 0 for task_id in self.tasks}
		for task in self.tasks.values():
			for dep in task.dependencies:
				indegree[task.id] += 1

		queue = deque(task_id for task_id, degree in indegree.items() if degree == 0)
		visited = 0

		while queue:
			current = queue.popleft()
			visited += 1
			for task in self.tasks.values():
				if current in task.dependencies:
					indegree[task.id] -= 1
					if indegree[task.id] == 0:
						queue.append(task.id)

		return visited != len(self.tasks)

	def topological_sort(self) -> List[str]:
		"""Return tasks ordered by dependency prerequisites."""

		indegree: Dict[str, int] = {task_id: 0 for task_id in self.tasks}
		adjacency: Dict[str, List[str]] = {task_id: [] for task_id in self.tasks}

		for task in self.tasks.values():
			for dep in task.dependencies:
				adjacency[dep].append(task.id)
				indegree[task.id] += 1

		queue = deque(task_id for task_id, degree in indegree.items() if degree == 0)
		order: List[str] = []

		while queue:
			node = queue.popleft()
			order.append(node)
			for neighbor in adjacency[node]:
				indegree[neighbor] -= 1
				if indegree[neighbor] == 0:
					queue.append(neighbor)

		if len(order) != len(self.tasks):
			raise CycleError("Cycle detected during topological sort")

		return order


def build_run_tasks(dag: DAG, run_id: str) -> List[Dict[str, Any]]:
	"""Build per-run task payloads for enqueuing."""

	dag.validate()
	adjacency: Dict[str, List[str]] = {task_id: [] for task_id in dag.tasks}
	for task in dag.tasks.values():
		for dep in task.dependencies:
			adjacency[dep].append(task.id)
	payloads: List[Dict[str, Any]] = []
	for task_id, task in dag.tasks.items():
		dependencies = list(task.dependencies)
		payloads.append(
			{
				"task_run_id": f"{run_id}:{task_id}:0",
				"run_id": run_id,
				"task_id": task_id,
				"dag_id": dag.id,
				"command": task.command,
				"callable": task.callable,
				"attempt": 0,
				"retries": task.retries,
				"retry_delay_seconds": task.retry_delay_seconds,
				"dependencies": dependencies,
				"downstream": adjacency[task_id],
				"timeout_seconds": task.timeout_seconds,
				"metadata": dict(task.metadata),
			}
		)
	return payloads
