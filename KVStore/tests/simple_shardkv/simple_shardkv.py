from KVStore.clients.clients import SimpleClient
from KVStore.tests.utils import test_get, test_put, test_append, test_l_pop, test_r_pop


class SimpleShardkvTests:
    def __init__(self, client: SimpleClient):
        self.client = client

    def test(self):
        assert (test_get(self.client, 0, None))

        assert (test_put(self.client, 2, "abcd"))
        assert (test_get(self.client, 2, "abcd"))

        assert(test_put(self.client, 0, "cs131!"))
        assert(test_get(self.client, 0, "cs131!"))

        assert (test_append(self.client, 1, "huh?"))
        assert (test_get(self.client, 1, "huh?"));
        assert (test_put(self.client, 1, "huh!"))
        assert (test_get(self.client, 1, "huh!"))
        assert (test_append(self.client, 1, "?"))
        assert (test_get(self.client, 1, "huh!?"))

        assert(test_l_pop(self.client, 2, None))
        assert(test_l_pop(self.client, 1, "h"))
        assert(test_r_pop(self.client, 1, "?"))
        assert(test_l_pop(self.client, 1, "u"))
        assert(test_r_pop(self.client, 1, "!"))
        assert(test_r_pop(self.client, 1, "h"))
        assert(test_l_pop(self.client, 1, None))

        assert(test_get(self.client, 1034, None));
        assert(test_get(self.client, 39, None));
        assert(test_append(self.client, 1034, "URV_ROCKS"));
        assert(test_append(self.client, 34, "paxos_enjoyer"));
        assert (test_get(self.client, 1034, "URV_ROCKS"));
        assert (test_get(self.client, 34, "paxos_enjoyer"));


