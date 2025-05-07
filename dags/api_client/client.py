from __future__ import annotations
import os
from airflow.sdk.api.client import Client, ServerResponseError
import requests
from typing import Optional
from methodtools import lru_cache
from http import HTTPStatus


def get_client_instance():
    AIRFLOW_API_URL = os.getenv(
        "AIRFLOW_API_URL", "http://airflow-apiserver:8080/api/v2"
    )
    return BasicAuthClient(AIRFLOW_API_URL)


class BasicAuthClient(Client):

    def __init__(self, base_url: Optional[str] = None, dry_run: bool = False, **kwargs):
        token = self._get_token(base_url)
        super().__init__(base_url=base_url, dry_run=dry_run, token=token, **kwargs)

    @staticmethod
    def _get_token(base_url):
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
        try:
            response = requests.post(auth_url, headers=headers, json=data)
        except:
            raise ValueError(f"{username=}\n{password=}\n{base_url=}\n{auth_url=}")
        if response.status_code != 201:
            print("Failed to get token:", response.status_code, response.text)
        jwt_token = response.json().get("access_token")
        return jwt_token

    @lru_cache()
    @property
    def pools(self) -> PoolOperations:
        return PoolOperations(self)


class BaseOperations:
    __slots__ = ("client",)

    def __init__(self, client: Client):
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

        try:
            return self.client.post("pools", json=pool_data)
        except ServerResponseError as e:
            if e.response.status_code == HTTPStatus.CONFLICT:
                return self.client.patch(f"pools/{name}", json=pool_data)
            else:
                raise
        except:
            raise ValueError("random value error")
