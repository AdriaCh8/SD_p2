from concurrent import futures
from KVStore.shardmaster.shardmaster import ShardMasterServicer
from KVStore.protos import kv_store_shardmaster_pb2_grpc
import grpc
import time
from multiprocessing import Process

HOSTNAME: str = "localhost"


def _run(port: int):
    address: str = "%s:%d" % (HOSTNAME, port)

    master_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = ShardMasterServicer()
    kv_store_shardmaster_pb2_grpc.add_KVStoreServicer_to_server(servicer, master_server)

    # listen on port 50051
    print("KV Storage server listening on: %s" % address)
    master_server.add_insecure_port(address)
    master_server.start()

    try:
        time.sleep(3000)
    except KeyboardInterrupt:
        master_server.stop(0)
    except EOFError:
        master_server.stop(0)


def run(port: int) -> Process:
    server_proc = Process(target=_run, args=[port])
    server_proc.start()
    return server_proc
