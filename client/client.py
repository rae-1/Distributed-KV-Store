import os
import rpyc
import yaml
import logging

path = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig( level=logging.DEBUG, filename=f"{path}/client.log", filemode='w')

class KVClient():
    def __init__(self):
        with open(file="client/client_config.yml", mode='r', encoding="utf-8") as file:
            config = yaml.safe_load(file)
            self.loadBalancerHost = config["lb_host"]
            self.loadBalancerPort = config["lb_port"]
        self.conn = rpyc.connect(self.loadBalancerHost, self.loadBalancerPort)

    def kv_init(self, server_list: list) -> None:
        return self.conn.root.exposed_init(server_list)

    def kv_get(self, key: str) -> any:
        return self.conn.root.exposed_get(key)

    def kv_put(self, key: str, value: any) -> any:
        return self.conn.root.exposed_put(key, value)

    def kv_shutdown(self) -> None:
        return self.conn.root.exposed_destory()
    
    
if __name__ == '__main__':
    client = KVClient()
    while True:
        print("""
          Operations:
            1. Initialize the servers
            2. Get the value for a particular key
            3. Put the value for a particular key
            4. Shutdown the servers
          """)
        choice: int = int(input("enter your choice:"))
        match choice:
            case 1: # init
                # server_list = ["127.0.0.1:8000", "localhost:9000"]
                server_list = ["localhost:9000"]
                response = client.kv_init(server_list)
                print(f"status_code: {response}")
            case 2: # GET
                key = input("Enter the key:")
                print(client.kv_get(key))
            case 3: # PUT
                key = input("Enter the key:")
                value = input("Enter the value:")
                status_code = client.kv_put(key, value)
                print(f"status_code: {status_code}")
            case 4: # Shutdown
                client.kv_shutdown()
            case _: # Exit
                print("Invalid choice.")
                break