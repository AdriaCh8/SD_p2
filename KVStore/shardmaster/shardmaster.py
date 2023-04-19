import google

from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, LeaveRequest, QueryResponse, JoinRequest
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer


class ShardMasterService:
    def __init__(self):
        """
        To fill with your code
        """

    def join(self, server: str):
        """
        :return:
        """

    def leave(self, servers: list):
        """

        :return:
        """

    def query(self, key: int) -> str:
        """

        :return:
        """


class ShardMasterSimpleServicer(ShardMasterServicer):
    def __init__(self):
        self.shard_master_service = ShardMasterService()

    def Join(self, request: JoinRequest, context) -> google.protobuf.Empty:
        """Missing associated documentation comment in .proto file."""

    def Leave(self, request: LeaveRequest, context) -> google.protobuf.Empty:
        """Missing associated documentation comment in .proto file."""

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        """Missing associated documentation comment in .proto file."""

