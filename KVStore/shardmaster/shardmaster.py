import logging

import grpc
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
from google.protobuf import empty_pb2

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
# Implement the shard master functionality, including handling storage server join and leave operations, 
# querying the server responsible for a key, and redistributing shards when storage servers join or leave the 
# system

class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        self.servers= dict() #server address with the range of keys
        self.serv=[] #

    def join(self, server: str):
        self.servers[server] = None  #add a server to the dict
        num_servers = len(self.servers) # do the calculus of nºkeys per server
        if num_servers != 0:  # avoid division by zero
            num_keys_per_server = 99 // num_servers
            self.redistributeKeysJoin(num_keys_per_server) #TODO: redistribute the keys of the other servers acordingly


    def leave(self, server: str):
        if server in self.servers:
            num_servers = len(self.servers)-1 #do the calculus of nºkeys per server
            if num_servers != 0:  # avoid division by zero
                num_keys_per_server = 99 // num_servers
                self.redistributeKeysLeave(num_keys_per_server, server) #redistribute the keys of the other servers acordingly
           
        
    def query(self, key: int) -> str:
        for server, key_range in self.servers.items():
            if key_range is not None and key_range[0] <= key <= key_range[1]:
                return server
        return ""

    def redistributeKeysJoin(self, keysPerServ: int):
        print(keysPerServ)
        sortedServers = sorted(self.servers.keys())
        antServer = None  # Initialize antServer variable
        initialupper_key = 0
        lowerVal = 0  # Define lowerVal variable
        upperVal = 0  # Define upperVal variable
        for i, server in enumerate(sortedServers):
            if antServer is not None:
                stub=self.getServer(antServer)
                stub.Redistribute(server, lowerVal, upperVal) #server -> server to wich the keys will be moved
            if self.servers[server] is not None:
                initialupper_key=self.servers[server][1]
            lowerKey = i * keysPerServ
            upperKey = (i + 1) * keysPerServ - 1
            #range of keys to be redistributed (upper_key - initialUpperKey)
            if upperKey>initialupper_key: 
                lowerVal = upperKey
                upperVal = initialupper_key
            else: 
                lowerVal = initialupper_key
                upperVal = upperKey
            self.servers[server] = (lowerKey, upperKey)
            antServer=server
    
    def redistributeKeysLeave(self, keysPerServ:int , serverLeave: str):
        stub=self.getServer(serverLeave)
        lowKey = self.servers[serverLeave][0]
        address=self.query(lowKey)
        stub.Redistribute(address, lowKey, self.servers[serverLeave][1]) #server -> server to wich the keys will be moved
        del self.servers[serverLeave] #delete the server of the dict
        sortedServers = sorted(self.servers.keys())
        antServer = None  # Initialize antServer variable
        initialupper_key = 0
        lowerVal = 0  # Define lowerVal variable
        upperVal = 0  # Define upperVal variable
        for i, server in enumerate(sortedServers):
            if antServer is not None:
                stub=self.getServer(antServer)
                stub.Redistribute(server, lowerVal, upperVal) #server -> server to wich the keys will be moved
            if self.servers[server] is not None:
                initialupper_key=self.servers[server][1]
            lowerKey = i * keysPerServ
            upperKey = (i + 1) * keysPerServ - 1
            #range of keys to be redistributed (upper_key - initialUpperKey)
            if upperKey>initialupper_key: 
                lowerVal = upperKey
                upperVal = initialupper_key
            else: 
                lowerVal = initialupper_key
                upperVal = upperKey
            self.servers[server] = (lowerKey, upperKey)
            antServer=server

    def getServer(self, address: str ) -> KVStoreStub:
        channel = grpc.insecure_channel(address)
        return KVStoreStub(channel)

class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        """
        To fill with your code
        """

    def query_replica(self, key: int, op: Operation) -> str:
        """
        To fill with your code
        """


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service
    

    def Join(self, request: JoinRequest, context) -> empty_pb2.Empty:
        self.shard_master_service.join(request.server)
        return empty_pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> empty_pb2.Empty:
        self.shard_master_service.leave(request.server)
        return empty_pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        return QueryResponse(server=self.shard_master_service.query(request.key))

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        return JoinReplicaResponse(role=self.shard_master_service.join_replica(request.server))

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        return QueryResponse(server=self.shard_master_service.query_replica(request.key, request.operation))