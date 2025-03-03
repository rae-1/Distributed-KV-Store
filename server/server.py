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

    def _async_persist_to_disk(self):
        thread = threading.Thread(target=self._persist_to_disk)
        thread.daemon = True
        thread.start()

    def _load_from_disk(self):
        try:
            with open("kv_store_backup.txt", "r") as f:
                self.store = eval(f.read())
        except FileNotFoundError:
            self.store = {}
    def ping_actual_server(self, host, port, timeout=0.5):
        try:
            conn = rpyc.connect(host, port, config={"sync_request_timeout": timeout})
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
            value = self.hinted_replica.get(key, None)[0]
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
                logging.debug(f"nextHost, nextPort: {nextHost}, {nextPort}")
                conn = rpyc.connect(nextHost, nextPort)
                if conn.root.ping():
                    value = conn.root.fetch(key, index<self.N)
                    outputs.append(value)
                    logging.debug(f"Server {nextHost}:{nextPort} returned value: {value}")
                else:
                    logging.debug(f"Node {nextHost}:{nextPort} is not active")
                conn.close()
            except Exception as e:
                logging.error(f"Error in Get: {e}")
                # return (None, -1)
            index += 1
            
        # Determine the majority value
        value_counts = {}
        for value in outputs:
            if value in value_counts:
                value_counts[value] += 1
            else:
                value_counts[value] = 1

        logging.debug(f"Value counts: {value_counts}")

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
    
    
    def exposed_put(self, key, value, target_host=None, target_port=None):
        """
        Store a key-value pair in the appropriate store.
        
        Args:
            key (str): The key to store
            value: The value to store
            target_host (str, optional): Target host for hinted handoff
            target_port (int, optional): Target port for hinted handoff
            
        Returns:
            int: 0 if key already existed, 1 if key is new, -1 on failure
        """
        
        try:
            if not self.active:
                logging.debug(f"Server is not active. Put operation rejected.")
                return -2
                
            logging.debug(f"Put request received for key: {key}, value: {value}")
            
            # If target_host and target_port are provided, this is a hinted handoff
            if target_host and target_port:
                exists = key in self.hinted_replica
                self.hinted_replica[key] = (value, target_host, target_port)
                logging.debug(f"Stored hinted handoff for key {key} intended for {target_host}:{target_port}")
            else:
                # Regular put operation
                exists = key in self.store
                self.store[key] = value
                self._async_persist_to_disk()
                logging.debug(f"Stored key {key} with value {value}")
            
            logging.debug("------"*4)
            return 0 if exists else 1
            
        except Exception as e:
            logging.error(f"Error in Put: {e}")
            return -1
    """
    def exposed_coordinator_put(self, key, value, intended_server_order):
        logging.debug(f"Coordinator put request received for key: {key}")
        logging.debug("------"*4)

        '''
        1. We have to maintain 2 arrays - working nodes and failed nodes {can store indices corresponding to intended_server_order}. Init exists = key in self.store.
        2. Find the starting index of (self.host, self.port) in intended_server_order. All indices before this index are failed nodes.
        3. Now iterate from start_index+1 to end of intended_server_order while success_count<N-1. 
        4. Ping the server[index]. If active, (i) index<N then send {key, value, is_primary=true} to server[index] and increment success_count. (ii) index>=N then send {key, value, is_primary=true, failed_nodes.start.value} to server[index], pop failed_nodes.start, increment success_count.
        5. Depending on whether primary is true (put in self.store) or false (put in self.hinted_replica along with target_host and target_port). Before putting we will check if the key exists in the corresponding store/hinted_replica. If it does we will return 0, else 1. Also persist the data to disk.
        6. The response value to send operation, if error-free, exists *= response.
        7. If success_count>0, then update the key's value in self.store and persist storage. Return exists.
        '''
        
        # 1. Initialize variables
        exists = key in self.store
        failed_nodes = []
        success_count = 0

        # 2. Find the starting index of (self.host, self.port) in intended_server_order
        intended_server_order = list(intended_server_order)
        start_index = intended_server_order.index((self.host, self.port))

        # All indices before start_index are failed nodes
        for i in range(start_index):
            failed_nodes.append(intended_server_order[i])

        # 3. Now iterate from start_index+1 to end of intended_server_order
        index = start_index + 1
        while index < len(intended_server_order) and success_count < self.N-1:
            try:
                nextHost, nextPort = intended_server_order[index]
                logging.debug(f"Trying to reach server {nextHost}:{nextPort}")
                
                # 4. Connect to the server
                conn = rpyc.connect(nextHost, nextPort)
                if index < self.N:
                    # This is a primary node, send direct put request
                    logging.debug(f"Sending direct put request to {nextHost}:{nextPort}")
                    response = conn.root.put(key, value)
                    
                    if response == -2:
                        # Server reported it's not active
                        logging.debug(f"Server {nextHost}:{nextPort} reported it's not active")
                        failed_nodes.append((nextHost, nextPort))
                    elif response == -1:
                        # Error occurred during put operation
                        logging.error(f"Error occurred during put operation at {nextHost}:{nextPort}")
                    else:
                        # Successfully stored - update exists value and increment success count
                        exists *= response
                        success_count += 1
                        logging.debug(f"Successfully stored key {key} at {nextHost}:{nextPort}")
                else:
                    # This is a node for hinted handoff
                    if failed_nodes:
                        failed_host, failed_port = failed_nodes.pop(0)
                        logging.debug(f"Sending hinted handoff for {failed_host}:{failed_port} to {nextHost}:{nextPort}")
                        response = conn.root.put(key, value, failed_host, failed_port)
                        
                        if response == -2:
                            # Server reported it's not active
                            logging.debug(f"Server {nextHost}:{nextPort} reported it's not active")
                            failed_nodes.append((nextHost, nextPort))
                            # Put back the failed node we popped
                            failed_nodes.append((failed_host, failed_port))
                        elif response == -1:
                            # Error occurred during put operation
                            logging.error(f"Error occurred during hinted handoff operation at {nextHost}:{nextPort}")
                            # Put back the failed node we popped
                            failed_nodes.append((failed_host, failed_port))
                        else:
                            # Successfully stored - update exists value and increment success count
                            exists *= response
                            success_count += 1
                            logging.debug(f"Successfully stored hinted handoff for {failed_host}:{failed_port} at {nextHost}:{nextPort}")

                conn.close()
                    
            except Exception as e:
                logging.error(f"Error connecting to server {nextHost}:{nextPort}: {e}")
                failed_nodes.append((nextHost, nextPort))
            
            index += 1  

        # 7. If success_count>0, then update the key's value in self.store and persist storage
        if success_count > 0:
            self.store[key] = value
            self._async_persist_to_disk()
            return exists
        else:
            return -1
    """
        
    # """
    def exposed_coordinator_put(self, key, value, replica_servers):
        logging.debug(f"Choosen as coordinator for key: {key}. Now, performing replication.")
        
        success_count = 1
        quorum_event = threading.Event()  # Event to signal when quorum is reached
        exists = 1  # dummy value assuming it exists

        logging.debug("Quorum event initialized")
        
        def replicate_to_server(host, port, target_info=None):
            nonlocal success_count
            nonlocal exists
            try:
                logging.debug(f"Pinging to {host}:{port}")
                conn = rpyc.connect(host, port)
                
                # If target_info is provided, this is a hinted handoff
                if target_info:
                    target_host, target_port = target_info
                    result = conn.root.put(key, value, target_host, target_port)
                else:
                    # Regular put operation
                    result = conn.root.put(key, value)
                
                if result != -1:
                    with self.lock:
                        exists *= result
                        success_count += 1
                        if success_count >= self.W:
                            logging.debug(f"Write quorum reached for key: {key}")
                            quorum_event.set()  # Signal that quorum is reached
                if quorum_event.is_set():
                    return True
                return True
            
            except Exception as e:
                logging.error(f"Failed to replicate to {host}:{port}: {e}")
                return False
    
        up_servers, down_servers = list(), list()
        replica_servers = list(replica_servers)
        index = replica_servers.index((self.host, self.port))

        logging.debug("Filling up_servers and down_servers")
        for servers in range(index):
            down_servers.append(replica_servers[servers])

        for i in range(index + 1, len(replica_servers)):
            logging.debug(f"Checking server {i}")
            try:
                if i == index:
                    logging.debug(f"Skipping current server {self.host}:{self.port}")
                    continue    # Skip the current server
                host, port = replica_servers[i]
                logging.debug(f"Pinging {host}:{port}")
                if self.ping_actual_server(host, port):
                    logging.debug(f"Server {host}:{port} is up")
                    up_servers.append((i, host, port))
                else:
                    down_servers.append((i, host, port))
            except Exception as e:
                down_servers.append((i, host, port))

        logging.debug(f"Down servers: {down_servers}")
        logging.debug(f"up servers: {up_servers}")

        if len(up_servers) < self.W:
            logging.error(f"Failed to reach write quorum for key: {key}")
            return -1
        
        # Checking if the current server is one of the intended servers
        logging.debug(f"Storing in either store or hinted_replicas")
        if index <= self.N:
            self.store[key] = value
            self._async_persist_to_disk()
        else:
            host, port = replica_servers[0]
            self.hinted_replica[key] = (value, host, port)
            logging.debug(f"Stored hinted handoff for key {key} intended for {host}:{port}")


        active_count = 0
        replication_tasks = dict()
        for i, host, port in up_servers:
            if i < self.N:
                logging.debug(f"i, N: {i}, {self.N}")
                replication_tasks[(host, port)] = None
                active_count += 1
            elif active_count < self.N - 1:
                # this will always work. Since, if it is not one of the intended servers,
                # then there must be one down_node infront of it and that server is intended one.
                logging.debug(f"active_count, N: {active_count}, {self.N}")
                logging.debug(f"len(down_servers): {len(down_servers)}")
                front_server = down_servers.pop(0)
                print(f"front_server: {front_server}")
                front_server = front_server[1:]
                replication_tasks[(host, port)] = front_server
                active_count += 1
        
        
        logging.debug("Sending replicas asynchronously")
        # with concurrent.futures.ThreadPoolExecutor(max_workers=len(replication_tasks)) as executor:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(replication_tasks))
        futures = [
            executor.submit(replicate_to_server, host, port, target_info) 
            for (host, port), target_info in replication_tasks.items()
        ]
        
        for future in concurrent.futures.as_completed(futures):
            if success_count >= self.W:
                break

        executor.shutdown(wait=False, cancel_futures=True)

            # # If we need more successes to reach quorum
            # if success_count < self.W:
            #     # Wait for quorum event with timeout
            #     quorum_reached = quorum_event.wait(timeout=5.0)
                
            #     if not quorum_reached:
            #         logging.error(f"Failed to reach write quorum for key: {key}")
            #         return -1
        
        # Return true if quorum was reached
        # return success_count >= self.W
        if success_count >= self.W:
            return exists
        return -1
    # """

    def exposed_delete(self, key):
        if key in self.store:
            del self.store[key]
            self._async_persist_to_disk()
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
