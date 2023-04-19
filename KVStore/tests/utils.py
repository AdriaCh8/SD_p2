import grpc
import time
from KVStore.protos.kv_store_pb2_grpc import *
from KVStore.protos.kv_store_pb2 import *
from KVStore.clients.clients import SimpleClient

TIMEOUT = 2
WAIT = 1
RETRIES = 5


def test_get(client: SimpleClient, key: int, expected_value: str) -> bool:
    def _get():
        try:
            value = client.get(key)
        except Exception as e:
            print(e)
            return False

        if expected_value is not None:
            return value == expected_value
        else:
            return value is None

    for _ in range(RETRIES):
        result: bool = _get()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_l_pop(client: SimpleClient, key: int) -> bool:
    def _l_pop():
        try:
            client.l_pop(key)
            return True
        except Exception as e:
            print(e)
            return False

    for _ in range(RETRIES):
        result: bool = _l_pop()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_r_pop(client: SimpleClient, key: int) -> bool:
    def _r_pop():
        try:
            client.l_pop(key)
            return True
        except Exception as e:
            print(e)
            return False

    for _ in range(RETRIES):
        result: bool = _r_pop()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_put(client: SimpleClient, key: int, value: str) -> bool:
    def _put():
        try:
            client.put(key, value)
            return True
        except Exception as e:
            print(e)
            return False

    for _ in range(RETRIES):
        result: bool = _put()
        if result is True:
            return True
        time.sleep(WAIT)
    return False


def test_append(client: SimpleClient, key: int, value: str) -> bool:
    def _append():
        try:
            client.append(key, value)
            return True
        except Exception as e:
            print(e)
            return False

    for _ in range(RETRIES):
        result: bool = _append()
        if result is True:
            return True
        time.sleep(WAIT)
    return False
