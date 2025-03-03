import os
import logging
import yaml
import rpyc
import random as rnd
from hashlib import md5
from typing import Dict, List
from rpyc.utils.server import ThreadedServer

curPath: str = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.DEBUG, filename=f"{curPath}/LB.log", filemode='w')

# NOTE: mapping to ring > done
# NOTE: finding the coordinator node > done
# NOTE: generating the routing table for each server > done
# TODO: sending the routing table to the servers
# TODO: integration with the server

# TODO: SQL integration
# TODO: hinted handoff
# TODO: testbench
# TODO: VCN: Version Control Number


# TODO: (IP, port) mapping: ping, routingTable

class consistentHashing(rpyc.Service):
    def __init__(self):
        self.ring: Dict[int, any] = dict()
        self.sortedServers = list()                         # (serverHash, (host, port, vNodeNum))
        self.server_list: List = list()
        self.configPath = curPath + "/lb_config.yml"

        with open(file=self.configPath, mode='r', encoding="utf-8") as file:
            config = yaml.safe_load(file)
            self.vNode: int = config["vNodes"]
            self.hashRandom: bool = config["hashRandom"]
            self.N: int = config["N"]                       # Replication factor

    def _createHash(self, key: str, random: bool = False) -> int:
        # if random:
        #     # Add some noise to the key to make the outcome more random
        #     noise = rnd.randint(0, 2**32)
        #     key = f"{key}_{noise}"

        hexHash = (md5(string=key.encode("utf-8"))).hexdigest()
        intHash = int(hexHash, base=16)
        return abs(intHash)

    def _translate_address(self, host, port):
        # Map localhost:900X to 172.16.238.1X:9000
        if host == 'localhost' and int(port) >= 9001 and int(port) <= 9005:
            container_id = int(port) - 9000
            return f"172.16.238.1{container_id}", 9000
        return host, port

    def _createRoutingTable(self) -> None:
        logging.debug("Creating the routing table for the servers.")

        # Initialize the routing table dictionary
        routingTables = { (host, port): [[] for _ in range(self.vNode)] for _, (host, port, _) in self.sortedServers }

        # Iterate over each server in the sorted list of servers
        for serverIndex in range(len(self.sortedServers)):
            serverInfo = self.sortedServers[serverIndex][1]
            host, port, vNodeNum = serverInfo

            uniqueServers = set()
            # Traverse the sortedServers list in a circular manner
            for i in range(len(self.sortedServers)):
                index = (serverIndex + i) % len(self.sortedServers)
                nextServerInfo = self.sortedServers[index][1]
                nextHost, nextPort, _ = nextServerInfo

                if (nextHost, nextPort) not in uniqueServers:
                    uniqueServers.add((nextHost, nextPort))
                    routingTables[(host, port)][vNodeNum].append((nextHost, nextPort))

        # Log the routing tables for each server
        for (host, port), table in routingTables.items():
            logging.debug(f"Routing table for {host}:{port}: {table}")

        # Store the routing tables
        self.routingTables = routingTables
        
        # Send routing tables to each server
        for (host, port), table in routingTables.items():
            translated_table = [[self._translate_address(h, p) for h, p in entry] for entry in table]
            try:
                conn = rpyc.connect(host, port)
                conn.root.set_routing_table(translated_table)
                conn.close()
            except Exception as e:
                logging.error(f"Failed to send routing table to {host}:{port}: {e}")
        
        logging.debug("Routing table sent.")
        logging.debug("------"*4)

    def _createRing(self) -> None:
        logging.debug("Creating the ring.")
        for server_info in self.server_list:
            host, port = server_info.split(':')
            for vNodeNumber in range(self.vNode):
                serverId: str = f"{host}_{port}_{vNodeNumber}"
                ringIndex: int = self._createHash(serverId, self.hashRandom)
                self.ring[ringIndex] = (host, port, vNodeNumber)
        logging.debug("Ring creation done.")
        
        self.sortedServers = sorted(self.ring.items(), key=lambda x: x[0])
        logging.debug("------"*4)

    def _binarySearch(self, start, end, targetHash) -> int:
        while (start <= end):
            mid = int((end - start) / 2) + start
            if self.sortedServers[mid][0] == targetHash:
                return mid
            elif self.sortedServers[mid][0] < targetHash:
                start = mid + 1
            else:
                end = mid - 1
        
        if start == len(self.sortedServers):
            return 0
        return start   # just the greater one

    def _findCoordinatorServer(self, key: str):
        logging.debug("Looking for the coordinator node.")
        
        keyHash: int = self._createHash(key)
        logging.debug(f"keyHash: {keyHash}")
        length = len(self.sortedServers)
        index = self._binarySearch(0, length - 1, keyHash)
        logging.debug(self.sortedServers)
        logging.debug(f"index: {index}")

        logging.debug("Coordinator node found.")
        logging.debug("------"*4)
        return self.ring[self.sortedServers[index][0]]

    def _listServers(self) -> None:
        logging.debug("Listing down the servers.")
        for serverIndex, serverInfo in self.ring.items():
            logging.debug(f"{serverIndex} : {serverInfo}")
        logging.debug("Listing completed.")
        logging.debug("------"*4)

    def _remove_server(self, host, port) -> None:
        logging.debug("Removing the server.")
        for vNodeNumber in range(self.vNode):
            serverId: str = f"{host}_{port}_{vNodeNumber}"
            ringIndex: int = self._createHash(serverId)
            del self.ring[ringIndex]
        self.sortedServers = [server for server in self.sortedServers if server[1] != (host, port)]
        logging.debug("Server removed.")
        logging.debug("------"*4)

    def _add_server(self, host, port) -> None:
        logging.debug("Adding the server.")
        for vNodeNumber in range(self.vNode):
            serverId: str = f"{host}_{port}_{vNodeNumber}"
            ringIndex: int = self._createHash(serverId)
            self.ring[ringIndex] = (host, port, vNodeNumber)
        self.sortedServers.append((ringIndex, (host, port, vNodeNumber)))
        self.sortedServers = sorted(self.sortedServers, key=lambda x: x[0])
        logging.debug("Server added.")
        logging.debug("------"*4)

    def _ping(self, host, port) -> bool:
        try:
            conn = rpyc.connect(host, port)
            response = conn.root.ping()
            conn.close()
            if response != True:
                logging.debug(f"Server {host}:{port} is not active.")
                logging.debug("------"*4)
                return False
            logging.debug(f"Ping successful for {host}:{port}")
            logging.debug("------"*4)
            return True
        except Exception as e:
            logging.error(f"Ping error: {e}")
            logging.debug("------"*4)
            return False


    '''
        exposed endpoints.
    '''
    def exposed_init(self, server_list: List) -> int:
        """
        Initializes the server list.

        Args:
            server_list (List): List of servers. Each element is of the format (host, port).

        Returns:
            int: Status code indicating success or failure. 0 for success, -1 for failure.
        """
        try:
            self.server_list = server_list
            self._createRing()
            self._createRoutingTable()
            self._listServers()
        except Exception as e:
            logging.error(f"Error: {e}")
            return -1
        return 0
    
    def exposed_destroy(self) -> int:
        """
        Shuts down the connection to a server and frees state.

        Returns:
            int: Status code indicating success or failure. 0 for success, -1 for failure.
        """
        try:
            self.ring = dict()
            self.sortedServers = list()
            self.server_list: List = list()
        except Exception as e:
            logging.error(f"Error in destroy: {e}")
            return -1
        return 0

    def exposed_get(self, key: str) -> int:
        '''
        Retrives the value for the given key.

        Args:
            key (str): Key for which the value needs to be fetched.
        
        Returns:
            int: Status code. 0 for success, 1 if the key is not present, and -1 for failure.
        '''
        logging.debug("Get request received.")
        coordinatorServer = self._findCoordinatorServer(key)
        host, port, vNodeNum = coordinatorServer

        # Loop through the first N entries in the routing table for the coordinator server
        intended_server_order = self.routingTables[(host, port)][vNodeNum]
        translated_intended_server_order = [self._translate_address(h, p) for h, p in intended_server_order]
        logging.debug(f"Intended server order: {translated_intended_server_order}")
        logging.debug(f"Actual server order: {intended_server_order}")
        for i in range(self.N):
            try:
                nextHost, nextPort = intended_server_order[i]
                if self._ping(nextHost, nextPort):
                    logging.debug(f"Coordinator Host: {host}, Port: {port}, vNodeNum: {vNodeNum}")
                    conn = rpyc.connect(nextHost, nextPort)
                    response = conn.root.get(key, translated_intended_server_order)
                    conn.close()
                    logging.debug("Get request completed.")
                    return response
                else:
                    logging.debug(f"Server {nextHost}:{nextPort} is not active.")
            except IndexError:
                logging.error(f"Not enough entries in the routing table for {host}:{port}")
                break
            except Exception as e:
                logging.error(f"Error connecting to server {nextHost}:{nextPort}: {e}")

        logging.error("Failed to fetch the data. No active servers available.")
        return (None,-1)
    
    def exposed_put(self, key: str, value: any) -> int:
        '''
        Stores the value for the given key.

        Args:
            key (str): Key for which the value needs to be stored.
            value (any): Value to be stored.
        
        Returns:
            int: Status code. 0 for success, -1 for failure.
        '''
        logging.debug("Put request received.")
        coordinatorServer = self._findCoordinatorServer(key)
        host, port, vNodeNum = coordinatorServer

        # Loop through the first N entries in the routing table for the coordinator server
        intended_server_order = self.routingTables[(host, port)][vNodeNum]
        translated_intended_server_order = [self._translate_address(h, p) for h, p in intended_server_order]
        logging.debug(f"Intended server order: {translated_intended_server_order}")
        logging.debug(f"Actual server order: {intended_server_order}")
        for i in range(self.N):
            try:
                nextHost, nextPort = intended_server_order[i]
                if self._ping(nextHost, nextPort):
                    logging.debug(f"Coordinator Host: {host}, Port: {port}, vNodeNum: {vNodeNum}")
                    conn = rpyc.connect(nextHost, nextPort)
                    response = conn.root.coordinator_put(key, value, translated_intended_server_order)
                    conn.close()
                    logging.debug("Put request completed.")
                    return response
                else:
                    logging.debug(f"Server {nextHost}:{nextPort} is not active.")
            except IndexError:
                logging.error(f"Not enough entries in the routing table for {host}:{port}")
                break
            except Exception as e:
                logging.error(f"Error connecting to server {nextHost}:{nextPort}: {e}")

        logging.error("Failed to store the data. No active servers available.")
        return -1
    
    def exposed_toggle_server(self, host: str, port: int) -> None:
        """
        Toggles the server status.

        Args:
            host (str): Host of the server.
            port (int): Port of the server.
        """
        
        conn = rpyc.connect(host, int(port))
        conn.root.exposed_toggle_server()
        conn.close()

        logging.debug("Server toggled.")
        logging.debug("------"*4)


if __name__ == '__main__':
    # serverList: List = ["127.0.0.1:5000", "localhost:7000", "127.0.0.4:9000"]
    # lb = consistentHashing(serverList)
    # lb._createRing()
    # lb.listServers()
    # lb.findCoordinatorServer("123")
    # lb._createRoutingTable()

    port = 5000
    server = ThreadedServer(consistentHashing, port=port)
    print(f"Load Balancer running on port {port}")
    server.start()
