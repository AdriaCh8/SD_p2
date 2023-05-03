import time
from KVStore.kvstorage import start_storage_server
from KVStore.logger import setup_logger
from KVStore.tests.kvstore import *
from KVStore.tests.utils import SHARDMASTER_PORT

setup_logger()


NUM_CLIENTS = 3

print("*************Single node tests**************")

master_adress = f"localhost:{SHARDMASTER_PORT}"
server_proc = start_storage_server.run(SHARDMASTER_PORT)
time.sleep(0.5)

try:

    print("-Simple tests-")
    test1 = SimpleKVStoreTests(master_adress, 1)
    test1.test()

    print("-Parallel tests-")
    test2 = SimpleKVStoreParallelTests(master_adress, NUM_CLIENTS)
    test2.test()

    print("-Race condition tests-")
    test2 = SimpleKVStoreRaceTests(master_adress, NUM_CLIENTS)
    test2.test()

except KeyboardInterrupt:
    pass

print("\n\n...Terminating server")
server_proc.terminate()