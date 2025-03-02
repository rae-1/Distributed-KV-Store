import rpyc
import logging
import yaml
import os
import time
import threading
import concurrent.futures
from rpyc.utils.server import ThreadedServer

path = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.DEBUG, filename=path+"/server.log", filemode='w')

class KeyValueStoreService(rpyc.Service):
    def __init__(self):
        self.routing_table = None
        self.host = None
        self.port = None
        self.store = dict()
        self.hinted_replica = dict()  # {key: (value, host, port)} 
        self.active : bool = True
        self.N = 3
        self.W = 2
        self.R = 2
        self.lock = threading.RLock()
        self._start_hinted_handoff_manager()

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
    def ping_actual_server(self, host, port):
        try:
            conn = rpyc.connect(host, port)
            conn.close()
            return True
        except Exception as e:
            return False

    def _start_hinted_handoff_manager(self):
        thread = threading.Thread(target=self._hinted_handoff_manager)
        thread.daemon = True
        thread.start()
        logging.info("Hinted Handoff Manager started in the background")

    def _hinted_handoff_manager(self):
        while True:
            try:
                with self.lock:
                    pending_servers = set((host, port) for _, host, port in self.hinted_replica.values())

                for host, port in pending_servers:
                    logging.debug(f"Checking if {host}:{port} is back online")
                    if self.ping_actual_server(host, port):
                        logging.info(f"Server {host}:{port} is back online. Going to process hinted handoff...")
                        self._process_hinted_handoff(host, port)
            except Exception as e:
                logging.error(f"Error in hinted handoff manager: {e}")

            # sleep for 10 seconds before checking again
            time.sleep(10)

    def _process_hinted_handoff(self, host, port):
        keys_to_remove = []
        with self.lock:
            for key, (value, target_host, target_port) in self.hinted_replica.items():
                if host == target_host and port == target_port:
                    try:
                        logging.debug(f"Trying to send data to recovered server. Hinted handoff in process for key {key}")
                        conn = rpyc.connect(target_host, target_port)
                        response = conn.root.put(key, value)

                        if response != -1:
                            logging.debug(f"Hinted handoff for key {key} processed successfully to {target_host}:{target_port}")
                            keys_to_remove.append(key)
                        else:
                            logging.error(f"Failed to send hinted handoff for key {key} to {target_host}:{target_port}")
                    except Exception as e:
                        logging.error(f"Error sending hinted handoff for key {key} to {target_host}:{target_port}: {e}")
                        logging.debug("------"*4)
            # There can be multiple keys designated for the same server so continue

            for key in keys_to_remove:
                self.hinted_replica.pop(key, None)
        logging.debug("------"*4)


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
        intended_server_order = list(intended_server_order)
        logging.debug(f"type of intended_server_order {type(intended_server_order)}")
        logging.debug(f"intended_server_order: {intended_server_order}")
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
                if conn.root.ping():
                    value = conn.root.fetch(key, index<self.N)
                    outputs.append(value)
                    logging.debug(f"Server {nextHost}:{nextPort} returned value: {value}")
                else:
                    logging.debug(f"Node {nextHost}:{nextPort} is not active")
                conn.close()
                index += 1
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
            self.store[key] = value
            return 0
        except Exception as e:
            logging.error(f"Error in put: {e}")
            return -1
        # try:
        #     status_code = self.get(key)[1]
        #     if status_code == -1: # some error occurred
        #         return -1
        #     # in any other case, we can proceed with the put operation
        #     self.store[key] = value
        #     self._async_persist_to_disk()
        #     logging.debug(f"key stored and persisted")
        #     logging.debug("------"*4)
        #     return status_code
        
        # except Exception as e:
        #     logging.debug(f"Key: {key}, Value: {value}")
        #     logging.error(f"Error in put: {e}")
        #     logging.debug("------"*4)
        #     return -1

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
        self.host = table[0][0][0]
        self.port = table[0][0][1]
        logging.info(f"Received routing table: {self.routing_table}")

    def exposed_toggle_server(self):
        self.active = not self.active
    
    
    '''
    ////////////////////////////////////////////////////
    /////////////////// Hinted Handoff /////////////////
    ////////////////////////////////////////////////////
    '''
    
        
    def exposed_store_hinted_handoff(self, key, value, target_host, target_port):
        self.hinted_replica[key] = (value, target_host, target_port)
        logging.debug(f"Stored hinted handoff for key {key} intended for {target_host}:{target_port}")
        logging.debug("------"*4)
        return True
    '''
    //////////////////////////////////////////////////////
    //////////////////////////////////////////////////////
    //////////////////////////////////////////////////////
    '''

    def exposed_ping(self):
        '''
            To simulate a server down scenario

            Returns:
                bool: True if server is virtually active, False otherwise
        '''
        return self.active


if __name__ == "__main__":
    port = 9000
    service = KeyValueStoreService()
    server = ThreadedServer(service=service, port=port)
    print(f"KV Store Node running on port {port}...")
    server.start()
