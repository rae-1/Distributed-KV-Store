import rpyc
import logging
import yaml
import os
import threading
from rpyc.utils.server import ThreadedServer

path = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.DEBUG, filename=path+"/server.log", filemode='w')

class KeyValueStoreService(rpyc.Service):
    def __init__(self):
        self.store = dict()
        # self.routing_table = None
        self.active : bool = True
        self.hinted_replica = dict()  # {key: (value, host, port)}
        self.N = 3
        self.W = 2
        self.R = 2

    def _persist_to_disk(self):
        with open("kv_store_backup.txt", "w") as f:
            f.write(str(self.store))

    def _async_persist_to_dist(self):
        thread = threading.Thread(target=self._persist_to_disk)
        thread.daemon = True
        thread.start()

    def _load_from_disk(self):
        try:
            with open("kv_store_backup.txt", "r") as f:
                self.store = eval(f.read())
        except FileNotFoundError:
            self.store = {}


    '''
        Exposed Endpoints
    '''

    def exposed_fetch(self, key, is_primary):
        """
        Fetch a key from either the primary store or the hinted replica.
        
        Args:
            key (str): The key to look up
            is_primary (bool): If True, check self.store, otherwise check self.hinted_replica
        
        Returns:
            The value associated with the key, or None if not found
        """
        logging.debug(f"Fetch request received for key: {key}, is_primary: {is_primary}")
        
        if is_primary:
            value = self.store.get(key, None)
            logging.debug(f"Value found in primary store: {value}")
        else:
            value = self.hinted_replica.get(key, None)
            logging.debug(f"Value found in hinted replica: {value}")
        
        logging.debug("------"*4)
        return value

    def exposed_get(self, key, intended_server_order):
        logging.debug(f"Get request received for key: {key}")
        logging.debug("------"*4)

        # Find the starting index of (self.host, self.port) in intended_server_order
        index = intended_server_order.index((self.host, self.port))
        
        outputs = []
        value = self.store.get(key, None)
        logging.debug(f"Coordinator {self.host}:{self.port} found value: {value}")
        outputs.append(value)
        index += 1

        while index < len(intended_server_order) and len(outputs) < self.N:
            try:
                nextHost, nextPort = intended_server_order[index]
                conn = rpyc.connect(nextHost, nextPort)
                if conn.root.exposed_ping():
                    value = conn.root.exposed_fetch(key, index<self.N)
                    outputs.append(value)
                    logging.debug(f"Server {nextHost}:{nextPort} returned value: {value}")
                else:
                    logging.debug(f"Node {nextHost}:{nextPort} is not active")
                conn.close()
            except Exception as e:
                logging.error(f"Error in Get: {e}")
                return (None, -1)
            
        # Determine the majority value
        value_counts = {}
        for value in outputs:
            if value in value_counts:
                value_counts[value] += 1
            else:
                value_counts[value] = 1

        most_common_value = None
        max_count = 0
        for value, count in value_counts.items():
            if count > max_count:
                most_common_value = value
                max_count = count

        if most_common_value is None:
            return (None, 1)
        if max_count >= self.R:
            logging.debug(f"Get request completed with value: {most_common_value}")
            return (most_common_value, 0)
        else:
            logging.error("Failed to fetch the data. No majority value found.")
            return (None, -1)

    def exposed_put(self, key, value):
        logging.debug(f"Key: {key}, Value: {value}")
        try:
            status_code = self.exposed_get(key)[1]
            if status_code == -1: # some error occurred
                return -1
            # in any other case, we can proceed with the put operation
            self.store[key] = value
            self._async_persist_to_disk()
            logging.debug(f"key stored and persisted")
            logging.debug("------"*4)
            return status_code
        
        except Exception as e:
            logging.debug(f"Key: {key}, Value: {value}")
            logging.error(f"Error in put: {e}")
            logging.debug("------"*4)
            return -1

    def exposed_delete(self, key):
        if key in self.store:
            del self.store[key]
            self._async_persist_to_dist()
            return f"Deleted {key}"
        return "Key not found"

    def exposed_list_keys(self):
        return list(self.store.keys())
    
    def exposed_set_routing_table(self, table):
        self.routing_table = table
        self.host = table[0][0]
        self.port = table[0][1]
        logging.info(f"Received routing table: {self.routing_table}")

    def exposed_toggle_server(self):
        self.active = not self.active

    def exposed_ping(self):
        return self.active


if __name__ == "__main__":
    port = 9000
    server = ThreadedServer(KeyValueStoreService, port=port)
    print(f"KV Store Node running on port {port}...")
    server.start()
