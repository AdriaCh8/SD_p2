from typing import Union, Dict
import grpc
import logging
from KVStore.protos.kv_store_pb2 import GetRequest, PutRequest, GetResponse
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub
from typing import Union
from KVStore.clients.clients import ShardClient
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub

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
        self.stub.Get(GetRequest(key=key))


    def l_pop(self, key: int) -> Union[str, None]:
       self.stub.LPop(GetRequest(key=key))

    def r_pop(self, key: int) -> Union[str, None]:
        self.stub.RPop(GetRequest(key=key))

    def put(self, key: int, value: str):
        self.stub.Put(PutRequest(key=key, value=value))

    def append(self, key: int, value: str):
        self.stub.Append(PutRequest(key=key, value=value))

    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str):
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        """
        To fill with your code
        """

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def r_pop(self, key: int) -> Union[str, None]:
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

class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        #Asks for the value of a key (read)
        request = GetRequest(key=key)
        return self.stub.get(request)

    def l_pop(self, key: int) -> Union[str, None]:
        #Asks for the rightmost character of the value of the key. The returned character is deleted from the stored value.
        # (write)
        request = GetRequest(key=key)
        response = self.stub.l_pop(request)
        return response.value if response.success else None

    def r_pop(self, key: int) -> Union[str, None]:
        request = GetRequest(key=key)
        response = self.stub.r_pop(request)
        return response.value if response.success else None


    def put(self, key: int, value: str):
        # Saves the value into the key.
        # If the key exists already, its value gets overwritten. (write)
        request = PutRequest(key=key, value=value)
        self.stub.put(request)


    def append(self, key: int, value: str):
        # Concatenates the specified value to the leftmost end of the value in the key.
        # If the key does not exist, it saves the value into the key. (write)
        request = AppendRequest(key=key, value=value)
        self.stub.append(request)

