import os
import logging
import yaml
import rpyc

from hashlib import md5
from typing import Dict, List
from rpyc.utils.server import ThreadedServer

curPath: str = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level=logging.DEBUG, filename=f"{curPath}/LB.log", filemode='w')

# NOTE: mapping to ring > done
# NOTE: finding the coordinator node > done
# NOTE: generating the routing table for each server > done
# TODO: sending the routing table to the servers
# TODO: in the destroy function connections needs to be closed.
# TODO: integration with the server

class consistentHashing(rpyc.Service):
    def __init__(self):
        self.N: int = 2**128    # MD5 uses 128bits
        self.ring: Dict[int, any] = dict();
        self.sortedServers = list() # (serverHash, (host, port, vNodeNum))
        self.server_list: List = list()
        self.configPath = curPath + "/lb_config.yml"

        with open(file=self.configPath, mode='r', encoding="utf-8") as file:
            config = yaml.safe_load(file)
            self.vNode: int = config["vNodes"]

    def createHash(self, key: str):
        hexHash = (md5(string=key.encode("utf-8"))).hexdigest()
        intHash = int(hexHash, base=16)
        return abs(intHash)

    def createRoutingTable(self) -> None:
        logging.debug("Creating the routing table for the servers.")

        # HACK: can we find a better logic to construct the routing table.
        for serverIndex in range(len(self.sortedServers)):
            uniqueServers, routingTable = set(), list()
            for index, serverInfo in enumerate(self.sortedServers):
                serverInfo = serverInfo[1]
                if index == serverIndex:
                    continue
                host, port, id = serverInfo
                if (host, port) not in uniqueServers:
                    uniqueServers.add((host, port))
                    routingTable.append((host, port))
            logging.debug(f"Routing table for {self.sortedServers[serverIndex][1]}: {routingTable}")
            # TODO: need to send this information
        logging.debug("Routing table sent.")
        logging.debug("------"*4)

    def createRing(self) -> None:
        logging.debug("Creating the ring.")
        for server_info in self.server_list:
            host, port = server_info.split(':')
            for vNodeNumber in range(self.vNode):
                serverId: str = f"{host}_{port}_{vNodeNumber}"
                ringIndex: int = self.createHash(serverId)
                self.ring[ringIndex] = (host, port, vNodeNumber)
        logging.debug("Ring creation done.")
        
        tempSortedServers = sorted(self.ring.items(), key=lambda x: x[0])
        uniqueServers = set()
        for server in tempSortedServers:
            host, port, vNodeNumber = server[1]
            if (host, port) not in uniqueServers:
                uniqueServers.add((host, port))
                self.sortedServers.append(server)
        logging.debug("------"*4)

    def binarySearch(self, start, end, targetHash) -> int:
        while (start <= end):
            mid = int((end - start) / 2) + start
            if self.sortedServers[mid][0] == targetHash:
                return mid
            elif self.sortedServers[mid][0] < targetHash:
                start = mid + 1
            else:
                end = mid - 1;
        
        if start == len(self.sortedServers):
            return 0
        return start;   # just the greater one

    def findCoordinatorServer(self, key: str):
        logging.debug("Looking for the coordinator node.")
        
        keyHash: int = self.createHash(key)
        logging.debug(f"keyHash: {keyHash}")
        length = len(self.sortedServers)
        index = self.binarySearch(0, length - 1, keyHash)
        logging.debug(self.sortedServers)
        logging.debug(f"index: {index}")

        logging.debug("Coordinator node found.")
        logging.debug("------"*4)
        return self.ring[self.sortedServers[index][0]]

    def listServers(self) -> None:
        logging.debug("Listing down the servers.")
        for serverIndex, serverInfo in self.ring.items():
            logging.debug(f"{serverIndex} : {serverInfo}")
        logging.debug("Listing completed.")
        logging.debug("------"*4)

    def destory(self) -> None:
        self.ring = dict();
        self.sortedServers = list()
        self.server_list: List = list()


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
            self.createRing()
            self.listServers()
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
        coordinatorServer = self.findCoordinatorServer(key)
        host, port, _ = coordinatorServer
        # conn = rpyc.connect(host, port)
        # response = conn.root.get(key)
        # conn.close()
        logging.debug("Get request completed.")
        return port
    
    def exposed_put(self, key: str, value: any) -> int:
        logging.debug("Put request received.")
        coordinatorServer = self.findCoordinatorServer(key)
        host, port, _ = coordinatorServer
        # conn = rpyc.connect(host, port)
        # response = conn.root.put(key, value)
        # conn.close()
        logging.debug("Put request completed.")
        return 0


if __name__ == '__main__':
    # serverList: List = ["127.0.0.1:5000", "localhost:7000", "127.0.0.4:9000"]
    # lb = consistentHashing(serverList)
    # lb.createRing()
    # lb.listServers()
    # lb.findCoordinatorServer("123")
    # lb.createRoutingTable()

    port = 5000
    server = ThreadedServer(consistentHashing, port=port)
    print(f"Load Balancer running on port {port}")
    server.start()
