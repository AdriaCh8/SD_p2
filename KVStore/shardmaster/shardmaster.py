import logging
from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *

logger = logging.getLogger(__name__)




class ShardMasterService:
    def join(self, server: str):
        pass

    def leave(self, server: str):
        pass

    def query(self, key: int) -> str:
        pass

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        """
        To fill with your code
        """

    def join(self, server: str):
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def query(self, key: int) -> str:
        """
        To fill with your code
        """

#TODO: The server logic, in KVStore.shardmaster.shardmaster.ShardMasterReplicasService to provide
#the required functionalities (sharding and replicas)
class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        self.number_of_shards = number_of_shards
        self.replica_masters = {}  # Dictionary to store the replica masters and their key ranges
        self.secondary_replicas = {}  # Dictionary to store the secondary replicas per replica master

    def leave(self, server: str):
        if server in self.replica_masters:
            # Replica master is leaving, redistribute key ranges
            key_ranges = self.replica_masters.pop(server) #delete
            self.redistribute_key_ranges(key_ranges)

        else:
            # Secondary replica is leaving, remove from the replica master's secondary replicas
            replica_master = self.secondary_replicas[server] #find the replica master
            self.secondary_replicas.pop(server) #delete the secondary replica of the replicas dictionary
            replica_master.remove(server) #delete the secondary replica of the replica master dictionary

    def redistribute_key_ranges(self, key_ranges):
        num_replicas = len(self.replica_masters)
        if num_replicas > 0:
            # Calculate the number of key ranges per replica master
            key_ranges_per_replica = len(key_ranges) // num_replicas
            remainder = len(key_ranges) % num_replicas

            replica_masters = list(self.replica_masters.keys())
            new_key_ranges = {}

            start = 0
            for i, replica_master in enumerate(replica_masters):
                num_key_ranges = key_ranges_per_replica + (1 if i < remainder else 0)
                end = start + num_key_ranges
                new_key_ranges[replica_master] = key_ranges[start:end]
                start = end

            # Update the replica masters with their new key ranges
            self.replica_masters = new_key_ranges

    def join_replica(self, server: str) -> Role:
        if len(self.replica_masters) < self.number_of_shards:
            # New server becomes a replica master
            self.replica_masters[server] = []
            role = Role.REPLICA_MASTER
        else:
            # New server becomes a secondary replica
            role = Role.SECONDARY_REPLICA
        return role

    def query_replica(self, key: int, op: Operation) -> str:
        if op == Operation.WRITE:
            # For write operation, return the address of the corresponding shard's replica master
            for replica_master, key_ranges in self.replica_masters.items():
                for key_range in key_ranges:
                    if key >= key_range.start and key <= key_range.end:
                        return replica_master
        elif op == Operation.READ:
            # For read operation, return the address of either a replica master or a secondary replica
            if self.replica_masters:
                return list(self.replica_masters.keys())[0]  # Return the address of the first replica master
            else:
                return None  # No replica masters available, return None or handle it as per your requirement
        else:
            raise ValueError("Invalid operation type")

#TODO:  The server QueryReplica gRPC call, in KVStore.shardmaster.shardmaster.ShardMasterServicer.
class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        server = request.server
        self.shard_master_service.join(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        server = request.server
        self.shard_master_service.leave(server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        key = request.key
        result = self.shard_master_service.query(key)
        return QueryResponse(result=result)

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        server = request.server
        role = self.shard_master_service.join_replica(server)
        return JoinReplicaResponse(role=role)
    
    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        server_address = self.shard_master_service.query_replica(request.key, request.operation)
        response = QueryResponse(server_address=server_address)
        return response
