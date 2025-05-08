from __future__ import annotations
import os
from airflow.sdk.api.client import BearerAuth as AirflowBearerAuth
from typing import Optional
from methodtools import lru_cache
from http import HTTPStatus
import time
import httpx

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-apiserver:8080/api/v2")


class BearerAuth(AirflowBearerAuth):
    def __init__(
        self,
        token: str,
        expires_at: Optional[float] = None,
        leeway_seconds: float = None,
    ):
        if expires_at is None:
            twenty_four_hours = 60 * 60 * 24  # Default duration from airflow
            token_duration = os.getenv(
                "AIRFLOW__API_AUTH__JWT_EXPIRATION_TIME", twenty_four_hours
            )

            leeway_seconds = leeway_seconds or 30
            now = time.time()
            expires_at = now + token_duration + leeway_seconds

        self.expires_at = expires_at
        super().__init__(token=token)

    def is_valid(self):
        now = time.time()
        return now < self.expires_at


class AirflowAPIClient(httpx.Client):

    def __init__(self, **kwargs):
        token = self._get_token(AIRFLOW_API_URL)
        auth = BearerAuth(token)
        event_hooks = {"request": [self.renew_token]}
        super().__init__(
            base_url=AIRFLOW_API_URL, auth=auth, event_hooks=event_hooks, **kwargs
        )

    def _get_token(self, base_url):
        if base_url is None:
            return ""

        username = os.getenv("_AIRFLOW_WWW_USER_USERNAME")
        password = os.getenv("_AIRFLOW_WWW_USER_PASSWORD")
        base_for_auth = base_url.split("/api")[0]
        headers = {"Content-Type": "application/json"}
        data = {
            "username": username,
            "password": password,
        }

        auth_url = f"{base_for_auth}/auth/token"
        response = httpx.post(auth_url, headers=headers, json=data)

        if response.status_code != 201:
            print("Failed to get token:", response.status_code, response.text)
        jwt_token = response.json().get("access_token")
        return jwt_token

    def renew_token(self, request: httpx.Request):
        if not self.auth.is_valid():
            new_token = self._get_token(str(self.base_url))
            self.auth.token = new_token
            request.headers["Authorization"] = "Bearer " + self.auth.token

    @lru_cache()
    @property
    def pools(self) -> PoolOperations:
        return PoolOperations(self)

    @lru_cache()
    @property
    def dags(self) -> DagOperations:
        return DagOperations(self)

    @lru_cache()
    @property
    def dag_runs(self) -> DagRunOperations:
        return DagRunOperations(self)

    @lru_cache()
    @property
    def task_instances(self) -> TaskInstanceOperations:
        return TaskInstanceOperations(self)

    @lru_cache()
    @property
    def tasks(self) -> TaskOperations:
        return TaskOperations(self)


class BaseOperations:
    __slots__ = ("client",)

    def __init__(self, client: AirflowAPIClient):
        self.client = client


class PoolOperations(BaseOperations):

    def create_or_update_pool(
        self,
        name: str,
        slots: int,
        description: Optional[str] = None,
        include_deferred: bool = False,
    ):
        # Will override existing pool if it already exists
        pool_data = {
            "name": name,
            "slots": slots,
            "description": description,
            "include_deferred": include_deferred,
        }

        response = self.client.post("pools", json=pool_data)
        if response.status_code == HTTPStatus.CONFLICT:
            response = self.client.patch(f"pools/{name}", json=pool_data)
        return response.json()


class DagOperations(BaseOperations):

    def get_all_dags(self):
        return self.client.get("dags").json()


class DagRunOperations(BaseOperations):

    def get_most_recent_dag_run(self, dag_id: str):
        params = {"dag_id": dag_id, "limit": 1, "order_by": "logical_date"}
        return self.client.get(f"dags/{dag_id}/dagRuns", params=params).json()


class TaskInstanceOperations(BaseOperations):
    def get_task_instances_in_dag_run(self, dag_id: str, dag_run_id: str):
        return self.client.get(
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        ).json()


class TaskOperations(BaseOperations):
    def get_tasks(self, dag_id: str):
        return self.client.get(f"dags/{dag_id}/tasks").json()
