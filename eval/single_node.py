from KVStore.simple_shardkv import start_server
from KVStore.clients.clients import SimpleClient
from KVStore.tests.simple_shardkv import *

PORT = 52003

server_proc = start_server.run(PORT)

client = SimpleClient(f"localhost:{PORT}")

# create a stub (client)
test1 = SimpleShardkvTests(client)
test1.test()
test2 = SimpleShardkvParallelTests
test2.test()

server_proc.terminate()