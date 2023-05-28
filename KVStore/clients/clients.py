import threading
from typing import Union, Dict
import grpc
import logging
from KVStore.protos.kv_store_pb2 import AppendRequest, GetRequest, PutRequest, GetResponse
from google.protobuf import message
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub

logger = logging.getLogger(__name__)


def _get_return(ret: GetResponse) -> Union[str, None]:
    if ret.HasField("value"):
        return ret.value
    else:
        return None

class SimpleClient:
    def __init__(self, kvstore_address: str):
        self.channel = grpc.insecure_channel(kvstore_address)
        self.stub = KVStoreStub(self.channel)

    def get(self, key: int) -> Union[str, None]:
        result = self.stub.Get(GetRequest(key=key)).value
        if(result==''):
            result=None
        return result
        

    def l_pop(self, key: int) -> Union[str, None]:
        result = self.stub.LPop(GetRequest(key=key)).value
        if result=="None":
            result = None
        return result

    def r_pop(self, key: int) -> Union[str, None]:
        result = self.stub.RPop(GetRequest(key=key)).value
        if result=="None":
            result = None
        return result

    def put(self, key: int, value: str):
        self.stub.Put(PutRequest(key=key, value=value))

    def append(self, key: int, value: str):
        self.stub.Append(AppendRequest(key=key, value=value))

    def stop(self):
        self.channel.close()


# Implement a client class that interacts with the shard master to obtain the address of the 
# appropriate storage server for a given key. 
# The client should then direct storage requests to the received server.
class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str):
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
       
    def get(self, key: int) -> Union[str, None]:
        if(key<100):
            stub = self.getServer(key)
            result = stub.Get(GetRequest(key=key)).value
            if(result==''):
                result=None
            return result


    def l_pop(self, key: int) -> Union[str, None]:
        stub=self.getServer(key)
        result = stub.LPop(GetRequest(key=key)).value
        if result=="None":
            result = None
        return result


    def r_pop(self, key: int) -> Union[str, None]:
        stub = self.getServer(key)
        result = stub.RPop(GetRequest(key=key)).value
        if result=="None":
            result = None
        return result


    def put(self, key: int, value: str):
       stub = self.getServer(key)
       stub.Put(PutRequest(key=key, value=value))


    def append(self, key: int, value: str):
        stub = self.getServer(key)
        stub.Append(AppendRequest(key=key, value=value))

    def getServer(self, key: int) -> KVStoreStub:
        a=self.stub.Query(QueryRequest(key=key))
        address = a.server
        channel = grpc.insecure_channel(address)
        return KVStoreStub(channel)
   

class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        #Asks for the value of a key (read)
        stub =self.getServer(key, Operation.GET)
        return stub.Get(GetRequest(key=key))

    def l_pop(self, key: int) -> Union[str, None]:
        #Asks for the rightmost character of the value of the key. The returned character is deleted from the stored value.
        # (write)
        stub =self.getServer(key, Operation.L_POP)
        result = stub.LPop(GetRequest(key=key)).value
        if result=="None":
            result = None
        return result

    def r_pop(self, key: int) -> Union[str, None]:
        stub =self.getServer(key, Operation.R_POP)
        result = stub.RPop(GetRequest(key=key))
        if result=="None":
            result = None
        return result


    def put(self, key: int, value: str):
        # Saves the value into the key.
        # If the key exists already, its value gets overwritten. (write)
        stub =self.getServer(key, Operation.PUT)
        stub.Put(PutRequest(key=key, value=value))


    def append(self, key: int, value: str):
        # Concatenates the specified value to the leftmost end of the value in the key.
        # If the key does not exist, it saves the value into the key. (write)
        stub =self.getServer(key, Operation.APPEND)
        stub.Append(AppendRequest(key=key, value=value))


    def getServer(self, key: int, op) -> KVStoreStub:
        a=self.stub.QueryReplica(QueryReplicaRequest(key=key, operation=op))
        address = a.server
        channel = grpc.insecure_channel(address)
        return KVStoreStub(channel)
