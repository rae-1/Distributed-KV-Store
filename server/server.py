import rpyc
import logging
import yaml
import os
from rpyc.utils.server import ThreadedServer

path = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.DEBUG, filename=path+"/server.log", filemode='w')

class KeyValueStoreService(rpyc.Service):
    def __init__(self):
        self.store = dict()

    def _persist_to_disk(self):
        with open("kv_store_backup.txt", "w") as f:
            f.write(str(self.store))

    def _load_from_disk(self):
        try:
            with open("kv_store_backup.txt", "r") as f:
                self.store = eval(f.read())
        except FileNotFoundError:
            self.store = {}


    '''
        Exposed Endpoints
    '''
    def exposed_get(self, key):
        logging.debug(f"Key: {key}")
        logging.debug("------"*4)
        try:
            value = self.store.get(key, 1)
            if (value == 1):
                return (value, 1)
            return (value, 0)
        except Exception as e:
            logging.debug(f"Key: {key}")
            logging.error(f"Error in get: {e}")
            return (value, -1)

    def exposed_put(self, key, value):
        logging.debug(f"Key: {key}, Value: {value}")
        try:
            status_code = self.exposed_get(key)[1]
            if status_code == -1: # some error occurred
                return -1
            # in any other case, we can proceed with the put operation
            self.store[key] = value
            self._persist_to_disk()
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
            self._persist_to_disk()
            return f"Deleted {key}"
        return "Key not found"

    def exposed_list_keys(self):
        return list(self.store.keys())


if __name__ == "__main__":
    port = 9000
    server = ThreadedServer(KeyValueStoreService, port=port)
    print(f"KV Store Node running on port {port}...")
    server.start()
