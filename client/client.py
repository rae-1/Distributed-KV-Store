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
    
    def _is_valid_key(self, key: str) -> bool:
        """
        Validates if the key follows the required rules:
        - Valid printable ASCII strings without special characters
        - 128 or less bytes in length
        - Cannot include "[" or "]" characters
        """
        if len(key.encode('ascii', 'ignore')) > 128:
            return False
        
        if '[' in key or ']' in key:
            return False
        
        # Check if all characters are printable ASCII without special characters
        return all(32 <= ord(c) <= 126 and c.isalnum() or c.isspace() for c in key)

    def _is_valid_value(self, value: str) -> bool:
        """
        Validates if the value follows the required rules:
        - Valid printable ASCII strings with neither special characters nor UU encoded characters
        - 2048 or less bytes in length
        """
        if len(value.encode('ascii', 'ignore')) > 2048:
            return False
        
        # Check if all characters are printable ASCII without special characters
        return all(32 <= ord(c) <= 126 and c.isalnum() or c.isspace() for c in value)

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
                if not client._is_valid_key(key):
                    print("Invalid key! Keys must be:\n- ASCII printable characters\n- No special characters\n- Max 128 bytes\n- No '[' or ']'")
                    continue

                value, status_code = client.kv_get(key)
                if status_code == 0:
                    print(f"Value: {value}")
                else:
                    print("Key not found!")
                print(f"status_code: {status_code}")

            case 3: # PUT
                key = input("Enter the key:")
                if not client._is_valid_key(key):
                    print("Invalid key! Keys must be:\n- ASCII printable characters\n- No special characters\n- Max 128 bytes\n- No '[' or ']'")
                    continue
                value = input("Enter the value:")
                if not client._is_valid_value(value):
                    print("Invalid value! Values must be:\n- ASCII printable characters\n- No special characters\n- Max 2048 bytes")
                    continue

                status_code = client.kv_put(key, value)
                if status_code != -1:
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