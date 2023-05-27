import time
import random
from typing import Dict, Union, List
import logging
import grpc
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub

from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


class KVStorageService:

    def __init__(self):
        pass

    def get(self, key: int) -> str:
        pass

    def l_pop(self, key: int) -> str:
        pass

    def r_pop(self, key: int) -> str:
        pass

    def put(self, key: int, value: str):
        pass

    def append(self, key: int, value: str):
        pass

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        pass

    def transfer(self, keys_values: list):
        pass

    def add_replica(self, server: str):
        pass

    def remove_replica(self, server: str):
        pass


class KVStorageSimpleService(KVStorageService):

    def __init__(self):
        self.values_set = dict()

    def get(self, key: int) -> Union[str, None]:
        return self.values_set.get(key)

    def l_pop(self, key: int) -> Union[str, None]:
        value = self.values_set.get(key)
        if(value==None):
            return None
        if(len(value)==0):
            return ""
        else:
            val = value[0]
            value[0] = ""
            self.values_set.update({key:value}) 
            return val

    def r_pop(self, key: int) -> Union[str, None]:
        value = self.values_set.get(key)
        if(value==None):
            return None
        if(len(value)==0):
            return ""
        else:
            val = value[-1]
            value[-1] = ""
            self.values_set.update({key:value}) 
            return val

    def put(self, key: int, value: str):
        self.values_set[key] = value

    def append(self, key: int, value: str):
        val = self.values_set.get(key)
        if(val==None):
            self.values_set.update({key:value}) 
        else:
            val = val + value
            self.values_set.update({key:val}) 

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        for i in self.values_set:
            if(i>=lower_val and i<=upper_val):
                destination_server.put(i,self.values_set[i])


    def transfer(self, keys_values: List[KeyValue]):
        transfered_values = dict()
        for i in keys_values:
            transfered_values.update({i:self.values_set[i]})
        return transfered_values


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
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

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context) -> GetResponse:
        self.storage_service.get(request.key)

    def LPop(self, request: GetRequest, context) -> GetResponse:
        self.storage_service.l_pop(request.key)

    def RPop(self, request: GetRequest, context) -> GetResponse:
        self.storage_service.r_pop(request.key)

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.put(request.key, request.value)

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.append(request.key, request.value)

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.redistribute(request.destination_server, request.lower_val, request.upper_val)

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.transfer(request.keys_values)

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.add_replica(request.server)

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.remove_replica(request.server)
