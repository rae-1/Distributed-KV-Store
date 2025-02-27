import os
import logging
import yaml

from hashlib import md5
from typing import Dict, List

logging.basicConfig(level=logging.DEBUG, filename="LB.log", filemode='w')

# NOTE: mapping to ring > done
# NOTE: finding the coordinator node > done
# NOTE: generating the routing table for each server > done
# TODO: sending the routing table to the servers
# TODO: in the destroy function connections needs to be closed.

class consistentHashing():
    def __init__(self, server_list):
        self.N: int = 2**128    # MD5 uses 128bits
        self.ring: Dict[int, any] = dict();
        self.sortedServers = list() # (serverHash, (host, port, vNodeNum))
        self.server_list: List = server_list
        self.configPath = os.path.dirname(os.path.abspath(__file__)) + "/config.yml"

        with open(file=self.configPath, mode='r', encoding="utf-8") as file:
            config = yaml.safe_load(file)
            self.vNode: int = config["vNodes"]

    def createHash(self, key: str):
        hexHash = (md5(string=key.encode("utf-8"))).hexdigest()
        intHash = int(hexHash, base=16)
        return abs(intHash)

    def createRoutingTable(self) -> None:
        logging.debug("Creating the routing table for the servers.")

        # HACK: need to find better logic to construct the routing table.
        for serverIndex in range(len(self.sortedServers)):
            uniqueServers, routingTable = set(), list()
            for index, serverHash, serverInfo in enumerate(self.sortedServers):
                if index == serverIndex:
                    continue
                host, port, id = serverInfo
                if (host, port) not in uniqueServers:
                    uniqueServers.add((host, port))
                    routingTable.append((host, port))

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
        self.sortedServers = sorted(self.ring.items(), key=lambda x: x[0])
        logging.debug(self.sortedServers)
        logging.debug("------"*4)

    def binarySearch(self, start, end, targetHash) -> int:
        while (start <= end):
            mid = ((end - start) / 2) + start
            if self.sortedServers[mid][0] == targetHash:
                return mid
            elif self.sortedServers[mid][0] < targetHash:
                start = mid + 1
            else:
                end = mid - 1;
        return start;   # just the greater one

    def findCoordinatorServer(self, key: str):
        logging.debug("Looking for the coordinator node.")
        keyHash: int = self.createHash(key)
        logging.debug(f"keyHash: {keyHash}")
        length = len(self.sortedServers)
        logging.debug("Coordinator node found.")
        logging.debug("------"*4)

        index = self.binarySearch(0, len - 1, keyHash)
        return self.ring[self.sortedServers[index]]

    def listServers(self) -> None:
        logging.debug("Listing down the servers.")
        for serverIndex, serverInfo in self.ring.items():
            print(serverIndex, ":", serverInfo)
        logging.debug("Listing completed.")
        logging.debug("------"*4)

    def destory(self) -> None:
        self.ring = dict();
        self.sortedServers = list()
        self.server_list: List = list()


if __name__ == '__main__':
    serverList: List = ["127.0.0.1:5000", "localhost:7000", "127.0.0.4:9000"]
    lb = consistentHashing(serverList)
    lb.createRing()
    lb.listServers()
    print(lb.N)