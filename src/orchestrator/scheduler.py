"""Scheduling logic for DAG runs."""

from __future__ import annotations

import copy
import logging
from typing import Dict, List

from .dag import DAG, build_run_tasks
from .persistence import PersistenceProtocol
from .utils import setup_logging


LOGGER = logging.getLogger(__name__)
setup_logging()


class Scheduler:
	"""Simple synchronous scheduler that seeds runnable tasks."""

	def __init__(self, persistence: PersistenceProtocol) -> None:
		self.persistence = persistence

	def schedule_dag(self, dag: DAG, run_id: str) -> None:
		"""Validate DAG and enqueue tasks that are dependency-free."""

		dag.validate()
		tasks = build_run_tasks(dag, run_id)
		blueprint: Dict[str, dict] = {task["task_id"]: copy.deepcopy(task) for task in tasks}

		runnable: List[dict] = []
		for task in tasks:
			task["dag_blueprint"] = blueprint
			task["attempt"] = 0
			task["task_run_id"] = f"{run_id}:{task['task_id']}:0"
			if not task["dependencies"]:
				runnable.append(task)

		run_metadata = {
			"dag_id": dag.id,
			"run_id": run_id,
			"task_count": len(tasks),
			"task_ids": list(dag.tasks.keys()),
		}
		LOGGER.info("Scheduling DAG %s run %s with %d tasks", dag.id, run_id, len(tasks))

		self.persistence.save_dag(dag.id, dag.model_dump_json())
		self._persist_run(run_id, run_metadata)

		for task in runnable:
			LOGGER.debug("Enqueueing initial task %s", task["task_id"])
			meta = {
				"task_id": task["task_id"],
				"run_id": run_id,
				"task_run_id": task["task_run_id"],
			}
			self.persistence.save_task_status(task["task_run_id"], "queued", meta)
			self.persistence.save_task_status(f"{run_id}:{task['task_id']}", "queued", meta)
			self.persistence.push_task_queue(task)

		# TODO: extend persistence of dependent tasks or metadata for richer scheduling.

	def _persist_run(self, run_id: str, metadata: Dict[str, int | str]) -> None:
		key = f"run:{run_id}"
		if hasattr(self.persistence, "save_task_status"):
			self.persistence.save_task_status(key, "scheduled", metadata)
