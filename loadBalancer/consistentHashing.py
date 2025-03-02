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

class consistentHashing(rpyc.Service):
    def __init__(self):
        self.N: int = 2**128    # MD5 uses 128bits
        self.ring: Dict[int, any] = dict()
        self.sortedServers = list() # (serverHash, (host, port, vNodeNum))
        self.server_list: List = list()
        self.configPath = curPath + "/lb_config.yml"

        with open(file=self.configPath, mode='r', encoding="utf-8") as file:
            config = yaml.safe_load(file)
            self.vNode: int = config["vNodes"]
            self.hashRandom: bool = config["hashRandom"]

    def _createHash(self, key: str, random: bool = False) -> int:
        if random:
            # Add some noise to the key to make the outcome more random
            noise = rnd.randint(0, 2**32)
            key = f"{key}_{noise}"

        hexHash = (md5(string=key.encode("utf-8"))).hexdigest()
        intHash = int(hexHash, base=16)
        return abs(intHash)

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
            # TODO: need to send this information to the servers
        
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

    def _destory(self) -> None:
        self.ring = dict()
        self.sortedServers = list()
        self.server_list: List = list()

    def _ping(self, host, port) -> bool:
        try:
            conn = rpyc.connect(host, port)
            conn.close()
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
            self._listServers()
        except Exception as e:
            logging.error(f"Error: {e}")
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
        host, port, _ = coordinatorServer
        logging.debug(f"Host: {host}, Port: {port}")

        status:int = self._ping(host, port)
        if (status == False):
            # need to remove this server from the ring temporarily.
            # and connect to the nearest server
            pass

        conn = rpyc.connect(host, port)
        response = conn.root.get(key)
        conn.close()
        logging.debug("Get request completed.")
        return response
    
    def exposed_put(self, key: str, value: any) -> int:
        logging.debug("Put request received.")
        coordinatorServer = self._findCoordinatorServer(key)
        host, port, _ = coordinatorServer
        logging.debug(f"Host: {host}, Port: {port}")
        conn = rpyc.connect(host, port)
        response = conn.root.put(key, value)
        conn.close()
        logging.debug("Put request completed.")
        return response


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
