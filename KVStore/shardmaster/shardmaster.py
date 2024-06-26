import logging
import threading

import grpc
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
from google.protobuf import empty_pb2
import KVStore.protos.kv_store_shardmaster_pb2 as CTE

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
         # Create a Semaphore with an initial value
        self.semaphore = threading.Semaphore() 


    def join(self, server: str):
        self.servers[server] = None  #add a server to the dict
        num_servers = len(self.servers) # do the calculus of nºkeys per server
        if num_servers != 0:  # avoid division by zero
            num_keys_per_server = 99 // num_servers
            remaining_keys = 99 % num_servers
            print(remaining_keys)
            self.semaphore.acquire()
            self.redistributeKeysJoin(num_keys_per_server, remaining_keys) #TODO: redistribute the keys of the other servers acordingly
            self.semaphore.release()

    def leave(self, server: str):
        if server in self.servers:
            num_servers = len(self.servers)-1 #do the calculus of nºkeys per server
            if num_servers != 0:  # avoid division by zero
                num_keys_per_server = 99 // num_servers
                remaining_keys = 99 % num_servers
                self.semaphore.acquire()
                self.redistributeKeysLeave(num_keys_per_server, server,remaining_keys ) #redistribute the keys of the other servers acordingly
                self.semaphore.release()
        
    def query(self, key: int) -> str:
        for server, key_range in self.servers.items():
            if key_range is not None and key_range[0] <= key <= key_range[1]:
                return server
        return ""
        
    def redistributeKeysJoin(self, keysPerServ: int,remainingKeys:int):
        sortedServers = sorted(self.servers.keys())
        antServer = None  # Initialize antServer variable
        initialupper_key = 0
        lowerVal = 0  # Define lowerVal variable
        upperVal = 0  # Define upperVal variable
        for i, server in enumerate(sortedServers):
            if antServer is not None:
                if i == len(sortedServers) - 1 and remainingKeys > 0:
                    upperVal += remainingKeys  # Add remaining keys to the last server
                stub=self.getServer(antServer)
                stub.Redistribute(RedistributeRequest(destination_server=server, lower_val=lowerVal, upper_val=upperVal)) #server -> server to wich the keys will be moved
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
            if i == len(sortedServers) - 1 and remainingKeys > 0:
                upperKey += remainingKeys  # Add remaining keys to the last server
            self.servers[server] = (lowerKey, upperKey)
            antServer=server
    
    def redistributeKeysLeave(self, keysPerServ:int , serverLeave: str, remainingKeys:int):
        stub=self.getServer(serverLeave)
        lowKey = self.servers[serverLeave][0]
        address=self.query(lowKey)
        stub.Redistribute(RedistributeRequest(destination_server=address,lower_val=lowKey, upper_val=self.servers[serverLeave][1])) #server -> server to wich the keys will be moved
        del self.servers[serverLeave] #delete the server of the dict
        sortedServers = sorted(self.servers.keys())
        antServer = None  # Initialize antServer variable
        initialupper_key = 0
        lowerVal = 0  # Define lowerVal variable
        upperVal = 0  # Define upperVal variable
        for i, server in enumerate(sortedServers):
            if antServer is not None:
                if i == len(sortedServers) - 1 and remainingKeys > 0:
                    upperVal += remainingKeys  # Add remaining keys to the last server
                stub=self.getServer(antServer)
                stub.Redistribute(RedistributeRequest(destination_server=server, lower_val=lowerVal, upper_val=upperVal)) #server -> server to wich the keys will be moved
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
            
            if i == len(sortedServers) - 1 and remainingKeys > 0:
                upperKey += remainingKeys  # Add remaining keys to the last server
            self.servers[server] = (lowerKey, upperKey)
            antServer=server

    def getServer(self, address: str ) -> KVStoreStub:
        channel = grpc.insecure_channel(address)
        return KVStoreStub(channel)

#TODO: The server logic, in KVStore.shardmaster.shardmaster.ShardMasterReplicasService to provide
#the required functionalities (sharding and replicas)
class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        self.numberOfShards = number_of_shards
        self.secondaryReplicas = dict()  # Dictionary to store the secondary replicas addresses(keys) per replica master(value) adress
        self.nOfReplicasPerRM = dict() #dictionary to store the number of replicas per replica master
 
    def leave(self, server: str):
        if server in self.servers:
            # Replica master is leaving, redistribute key ranges
            self.leave(server)
        else:
            # Secondary replica is leaving, remove from the replica master's secondary replicas
            self.nOfReplicasPerRM[self.secondaryReplicas[server]]-=1 #decrements the number of replicas of the server
            self.secondaryReplicas.pop(server)#delete the secondary replica of the replica master dictionary
            

    def join_replica(self, server: str) -> Role:
        if len(self.servers) < self.numberOfShards:
            # New server becomes a replica master
            self.servers[server] = []
            self.nOfReplicasPerRM[server]=0
            self.join(server)
            return CTE.MASTER
        else:
            # New server becomes a secondary replica of the replica master with least replicas
            min_frequency = min(self.nOfReplicasPerRM.values()) #find wich RM has less replicas
            repMWithLessR=None
            self.semaphore.acquire()
            for key in self.nOfReplicasPerRM:
                if self.nOfReplicasPerRM[key] == min_frequency:
                    repMWithLessR = key
                    break
            self.semaphore.release()
            self.secondaryReplicas[server]=repMWithLessR
            self.nOfReplicasPerRM[repMWithLessR]+=1
            stub=self.getServer(repMWithLessR)
            stub.AddReplica(ServerRequest(server=server))

            return CTE.REPLICA

    def query_replica(self, key: int, op: Operation) -> str:
        replica_m = None
        for replica_master in self.servers:
            key_range=self.servers[replica_master]
            if key >= key_range[0] and key <= key_range[1]:
                replica_m = replica_master
                break
        if op in (CTE.APPEND, CTE.L_POP, CTE.PUT, CTE.R_POP):
            # For write operation, return the address of the corresponding shard's replica master
            return replica_m
        elif op == CTE.GET:
            # For read operation, return the address of either a replica master or a secondary replica
            for replica_key, replica_val in self.secondaryReplicas.items():
                if replica_val == replica_m:
                    return replica_key
            return replica_m
        else:
            raise ValueError("Invalid operation type")



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