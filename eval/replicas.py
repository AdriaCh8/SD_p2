import time
from KVStore.kvstorage import start_storage_server_replicas
from KVStore.logger import setup_logger
from KVStore.shardmaster import start_shardmaster_replicas
from KVStore.tests.replication.replication_performance import ShardKVReplicationPerformanceTest
from tabulate import tabulate
from KVStore.tests.utils import SHARDMASTER_PORT, wait, get_port

setup_logger()

master_adress = f"localhost:{SHARDMASTER_PORT}"

NUM_CLIENTS = 2
NUM_SHARDS = 2
NUM_STORAGE_SERVERS = 6
CONSISTENCY_LEVELS = [0, 1, 2]

print("*************Sharded + replicas tests**************")

print("Testing throughput (OP/s) and number of errors for different consistency levels")
print("Configuration:")
print("\tNumber of clients: %d" % NUM_CLIENTS)
print("\tNumber of shards: %d" % NUM_SHARDS)
print("\tStorage servers: %d" % NUM_STORAGE_SERVERS)

results = []

for consistency_level in CONSISTENCY_LEVELS:
    print(f"Running with consistency level {consistency_level}.")

    server_proc = start_shardmaster_replicas.run(SHARDMASTER_PORT, NUM_SHARDS)
    wait()
    storage_proc_end_queues = [
        start_storage_server_replicas.run(get_port(), SHARDMASTER_PORT, consistency_level)
        for i in range(NUM_STORAGE_SERVERS)
    ]
    wait()

    test = ShardKVReplicationPerformanceTest(master_adress, NUM_CLIENTS)
    throughput, error_rate = test.test()
    results.append([consistency_level, throughput, error_rate])

    storage_proc_end_queues.reverse()
    [queue.put(0) for queue in storage_proc_end_queues[:-NUM_SHARDS]]
    wait()
    [queue.put(0) for queue in storage_proc_end_queues[(NUM_STORAGE_SERVERS-NUM_SHARDS):]]

    time.sleep(2)
    server_proc.terminate()
    wait()


print("Final results:")
# Show results in table
print(tabulate(results, headers=['Consistency_level', 'Throughput (OP/s)', 'Error rate (Errors/s)']))
