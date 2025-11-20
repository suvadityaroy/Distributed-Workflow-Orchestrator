"""Worker that executes scheduled tasks."""

from __future__ import annotations

import copy
import logging
import time
from typing import Any, Callable, Dict, Optional

from .executor import execute_task
from .persistence import PersistenceProtocol
from .utils import retry_backoff, setup_logging


LOGGER = logging.getLogger(__name__)
setup_logging()


class Worker:
	"""Polls the task queue and executes runnable tasks."""

	def __init__(
		self,
		persistence: PersistenceProtocol,
		executor: Callable[[Dict[str, Any], Optional[int]], Dict[str, Any]] = execute_task,
	) -> None:
		self.persistence = persistence
		self.executor = executor

	def run(self, loop_forever: bool = True) -> None:
		"""Start the worker loop."""

		LOGGER.info("Worker started; loop_forever=%s", loop_forever)
		try:
			while True:
				payload = self.persistence.pop_task_queue(timeout=5)
				if not payload:
					if not loop_forever:
						break
					continue

				LOGGER.info("Executing task %s", payload["task_run_id"])
				self._process_task(payload)
		except KeyboardInterrupt:  # pragma: no cover - manual interruption
			LOGGER.info("Worker shutdown requested")

	def _process_task(self, payload: Dict[str, Any]) -> None:
		task_run_id = payload["task_run_id"]
		timeout = payload.get("timeout_seconds")
		self._record_status(task_run_id, payload, "running", {})

		result = self.executor(payload, timeout)
		self._record_status(task_run_id, payload, result["status"], result)

		if result["status"] == "success":
			self._schedule_downstream(payload)
			return

		attempt = payload.get("attempt", 0)
		retries = payload.get("retries", 0)
		if attempt < retries:
			next_attempt = attempt + 1
			delay = retry_backoff(attempt)
			LOGGER.warning(
				"Retrying task %s (attempt %s/%s) in %.2fs",
				task_run_id,
				next_attempt,
				retries,
				delay,
			)
			time.sleep(delay)
			payload["attempt"] = next_attempt
			payload["task_run_id"] = f"{payload['run_id']}:{payload['task_id']}:{next_attempt}"
			self._mark_queued(payload)
			self.persistence.push_task_queue(payload)
		else:
			LOGGER.error("Task %s failed after %s attempts", task_run_id, attempt)

	def _schedule_downstream(self, payload: Dict[str, Any]) -> None:
		downstream = payload.get("downstream", [])
		if not downstream:
			return
		for child in downstream:
			child_payload = self._build_child_payload(payload, child)
			if not child_payload:
				continue
			if not self._dependencies_satisfied(payload["run_id"], child_payload):
				continue
			if self._already_scheduled(child_payload):
				continue
			LOGGER.debug("Enqueueing downstream task %s", child)
			self._mark_queued(child_payload)
			self.persistence.push_task_queue(child_payload)

	def _build_child_payload(self, parent_payload: Dict[str, Any], child_id: str) -> Dict[str, Any] | None:
		blueprint = parent_payload.get("dag_blueprint")
		if not blueprint:
			LOGGER.debug("Missing blueprint; cannot schedule downstream task %s", child_id)
			return None
		child_base = blueprint.get(child_id)
		if not child_base:
			LOGGER.debug("Blueprint does not contain task %s", child_id)
			return None
		child_payload = copy.deepcopy(child_base)
		child_payload["attempt"] = 0
		child_payload["task_run_id"] = f"{child_payload['run_id']}:{child_id}:0"
		child_payload["dag_blueprint"] = blueprint
		return child_payload

	def _dependencies_satisfied(self, run_id: str, child_payload: Dict[str, Any]) -> bool:
		for dep in child_payload.get("dependencies", []):
			status = self.persistence.get_task_status(f"{run_id}:{dep}")
			if status.get("status") != "success":
				return False
		return True

	def _record_status(
		self,
		task_run_id: str,
		payload: Dict[str, Any],
		status: str,
		meta: Dict[str, Any],
	) -> None:
		enriched_meta = {**meta, "task_id": payload["task_id"], "run_id": payload["run_id"]}
		self.persistence.save_task_status(task_run_id, status, enriched_meta)
		canonical_key = f"{payload['run_id']}:{payload['task_id']}"
		self.persistence.save_task_status(canonical_key, status, enriched_meta)

	def _already_scheduled(self, payload: Dict[str, Any]) -> bool:
		status = self.persistence.get_task_status(f"{payload['run_id']}:{payload['task_id']}")
		return status.get("status") in {"queued", "running", "success"}

	def _mark_queued(self, payload: Dict[str, Any]) -> None:
		meta = {"task_id": payload["task_id"], "run_id": payload["run_id"], "task_run_id": payload["task_run_id"]}
		canonical_key = f"{payload['run_id']}:{payload['task_id']}"
		self.persistence.save_task_status(payload["task_run_id"], "queued", meta)
		self.persistence.save_task_status(canonical_key, "queued", meta)
