from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer
from KVStore.protos.kv_store_pb2_grpc import google_dot_protobuf_dot_empty__pb2 as empty
from typing import Any


class KVStoreService:

    def __init__(self):
        """
        To fill with your code
        """

    def get(self, key: int) -> str:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def put(self, key: int, value: str):
        """
        To fill with your code
        """

    def append(self, key: int, value: str):
        """
        To fill with your code
        """

    def redistribute(self, destination: str, lower_val: int, upper_val: int):
        """
        To fill with your code
        """

    def transfer(self, keys_values: list):
        """
        To fill with your code
        """


class KVStorageSimpleServicer(KVStoreServicer):

    def __init__(self):
        self.storage_service = KVStoreService()
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context):
        """
        To fill with your code
        """

    def LPop(self, request: GetRequest, context):
        """
        To fill with your code
        """

    def RPop(self, request: GetRequest, context):
        """
        To fill with your code
        """

    def Put(self, request: PutRequest, context):
        """
        To fill with your code
        """

    def Append(self, request: AppendRequest, context):
        """
        To fill with your code
        """

    def Redistribute(self, request: RedistributeRequest, context):
        """
        To fill with your code
        """

    def Transfer(self, request: TransferRequest, context):
        """
        To fill with your code
        """
