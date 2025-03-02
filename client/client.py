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
            loadBalancerHost = config["lb_host"]
            loadBalancerPort = config["lb_port"]
            self.server_list = config["server_list"]
        self.conn = rpyc.connect(loadBalancerHost, loadBalancerPort)

    def kv_init(self) -> None:
        return self.conn.root.exposed_init(self.server_list)

    def kv_get(self, key: str) -> any:
        return self.conn.root.exposed_get(key)

    def kv_put(self, key: str, value: any) -> any:
        return self.conn.root.exposed_put(key, value)

    def kv_shutdown(self) -> None:
        return self.conn.root.exposed_destroy()
    
    # For testing
    def toggle_server(self, host: str, port: int) -> None:
        return self.conn.root.exposed_toggle_server(host, port)

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
                response = client.kv_init()
                print(f"status_code: {response}")
            case 2: # GET
                key = input("Enter the key:")
                value, status_code = client.kv_get(key)
                if status_code == 0:
                    print(f"Value: {value}")
                else:
                    print("Key not found.")
                print(f"status_code: {status_code}")
            case 3: # PUT
                key = input("Enter the key:")
                value = input("Enter the value:")
                status_code = client.kv_put(key, value)
                if status_code == 0:
                    print("Key-Value pair stored successfully.")
                print(f"status_code: {status_code}")
            case 4: # Shutdown
                client.kv_shutdown()
            case 5: # Toggle server
                host = input("Enter the host:")
                port = int(input("Enter the port:"))
                client.toggle_server(host, port)
            case _: # Exit
                print("Invalid choice.")
                break