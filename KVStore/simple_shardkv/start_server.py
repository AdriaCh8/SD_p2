from concurrent import futures
from KVStore.simple_shardkv.shardkv import KVStorageSimpleServicer
from KVStore.protos import kv_store_pb2_grpc
import grpc
import time
from multiprocessing import Process

HOSTNAME: str = "localhost"


def _run(port: int):
    address: str = "%s:%d" % (HOSTNAME, port)

    storage_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = KVStorageSimpleServicer()
    kv_store_pb2_grpc.add_KVStoreServicer_to_server(servicer, storage_server)

    # listen on port 50051
    print("KV Storage server listening on: %s" % address)
    storage_server.add_insecure_port(address)
    storage_server.start()

    try:
        time.sleep(3000)
    except KeyboardInterrupt:
        storage_server.stop(0)
    except EOFError:
        storage_server.stop(0)


def run(port: int) -> Process:
    server_proc = Process(target=_run, args=[port])
    server_proc.start()
    return server_proc

