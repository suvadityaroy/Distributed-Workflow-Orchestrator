"""Persistence layer for DAGs, task queue, and status tracking."""

from __future__ import annotations

import json
import os
import threading
import time
from queue import Queue, Empty
from typing import Dict, Optional, Protocol

import redis

from .utils import retry_backoff


class QueueInterface(Protocol):
	"""Minimal queue contract for scheduler/worker collaboration."""

	def push(self, item: dict) -> None:  # pragma: no cover - protocol
		...

	def pop(self, timeout: int = 5) -> Optional[dict]:  # pragma: no cover - protocol
		...


class PersistenceProtocol(QueueInterface, Protocol):
	"""Full persistence contract used across the application."""

	def save_dag(self, dag_id: str, dag_json: str) -> None:
		...

	def load_dag(self, dag_id: str) -> Optional[str]:
		...

	def push_task_queue(self, item: dict) -> None:
		...

	def pop_task_queue(self, timeout: int = 5) -> Optional[dict]:
		...

	def save_task_status(self, task_run_id: str, status: str, meta: dict) -> None:
		...

	def get_task_status(self, task_run_id: str) -> dict:
		...


class RedisPersistence(PersistenceProtocol):
	"""Redis-backed persistence implementation."""

	def __init__(
		self,
		redis_url: str,
		queue_key: str = "orchestrator:tasks",
		dag_prefix: str = "orchestrator:dag:",
		status_prefix: str = "orchestrator:status:",
	) -> None:
		self.redis_url = redis_url
		self.queue_key = queue_key
		self.dag_prefix = dag_prefix
		self.status_prefix = status_prefix
		self._client = self._connect()

	def _connect(self) -> redis.Redis:
		last_exc: Exception | None = None
		for attempt in range(3):
			try:
				client = redis.Redis.from_url(self.redis_url, decode_responses=True)
				client.ping()
				return client
			except redis.RedisError as exc:  # pragma: no cover - integration
				last_exc = exc
				time.sleep(min(retry_backoff(attempt), 10))
		raise ConnectionError("Unable to connect to Redis") from last_exc

	@property
	def client(self) -> redis.Redis:
		try:
			self._client.ping()
		except redis.RedisError:  # pragma: no cover - integration
			self._client = self._connect()
		return self._client

	# DAG storage ---------------------------------------------------------
	def save_dag(self, dag_id: str, dag_json: str) -> None:
		self.client.set(f"{self.dag_prefix}{dag_id}", dag_json)

	def load_dag(self, dag_id: str) -> Optional[str]:
		value = self.client.get(f"{self.dag_prefix}{dag_id}")
		return str(value) if value else None

	# Task queue ----------------------------------------------------------
	def push(self, item: dict) -> None:
		self.push_task_queue(item)

	def pop(self, timeout: int = 5) -> Optional[dict]:
		return self.pop_task_queue(timeout)

	def push_task_queue(self, item: dict) -> None:
		self.client.lpush(self.queue_key, json.dumps(item))

	def pop_task_queue(self, timeout: int = 5) -> Optional[dict]:
		result = self.client.brpop(self.queue_key, timeout=timeout)
		if result is None:
			return None
		_, payload = result
		return json.loads(payload)

	# Task status ---------------------------------------------------------
	def save_task_status(self, task_run_id: str, status: str, meta: dict) -> None:
		payload = {"status": status, **meta}
		self.client.set(f"{self.status_prefix}{task_run_id}", json.dumps(payload))

	def get_task_status(self, task_run_id: str) -> dict:
		payload = self.client.get(f"{self.status_prefix}{task_run_id}")
		return json.loads(payload) if payload else {}


class InMemoryPersistence(PersistenceProtocol):
	"""Thread-safe in-memory persistence for fast unit tests."""

	def __init__(self) -> None:
		self._queue: Queue[dict] = Queue()
		self._dags: Dict[str, str] = {}
		self._statuses: Dict[str, dict] = {}
		self._lock = threading.Lock()

	def save_dag(self, dag_id: str, dag_json: str) -> None:
		with self._lock:
			self._dags[dag_id] = dag_json

	def load_dag(self, dag_id: str) -> Optional[str]:
		with self._lock:
			return self._dags.get(dag_id)

	def push(self, item: dict) -> None:
		self.push_task_queue(item)

	def pop(self, timeout: int = 5) -> Optional[dict]:
		return self.pop_task_queue(timeout)

	def push_task_queue(self, item: dict) -> None:
		self._queue.put_nowait(item)

	def pop_task_queue(self, timeout: int = 5) -> Optional[dict]:
		try:
			return self._queue.get(timeout=timeout)
		except Empty:
			return None

	def save_task_status(self, task_run_id: str, status: str, meta: dict) -> None:
		with self._lock:
			self._statuses[task_run_id] = {"status": status, **meta}

	def get_task_status(self, task_run_id: str) -> dict:
		with self._lock:
			return self._statuses.get(task_run_id, {})


def get_persistence_from_env() -> PersistenceProtocol:
	"""Create a persistence backend based on environment configuration."""

	redis_url = os.getenv("REDIS_URL")
	if redis_url:
		return RedisPersistence(redis_url)
	return InMemoryPersistence()
