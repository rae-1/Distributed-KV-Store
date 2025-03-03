#!/usr/bin/env python3
import subprocess
import time
import os
import sys
import rpyc
import yaml
import signal
import threading

class KVStoreAutomation:
    def __init__(self):
        self.lb_process = None
        self.client = None
        
    def setup_environment(self):
        """Set up the Python environment"""
        print("Setting up environment...")
        if not os.path.exists('.venv'):
            subprocess.run(['python3', '-m', 'venv', '.venv'])
        
        # Activate virtual environment and install dependencies
        activate_cmd = ['source', '.venv/bin/activate']
            
        subprocess.run(f"{' '.join(activate_cmd)} && pip install -r requirements.txt", 
                      shell=True)
        print("Environment setup complete!")
        
    def start_docker_containers(self):
        """Start the Docker containers for servers"""
        print("Starting Docker containers...")
        subprocess.run(['docker-compose', 'down'], stdout=subprocess.PIPE)
        subprocess.run(['docker-compose', 'build'], stdout=subprocess.PIPE)
        subprocess.run(['docker-compose', 'up', '-d'])
        print("Docker containers started!")
        time.sleep(7)  # Give containers time to initialize
        
    def start_load_balancer(self):
        """Start the load balancer"""
        print("Starting load balancer...")
        self.lb_process = subprocess.Popen(
            ['python3', 'loadBalancer/consistentHashing.py'],
            stdout=subprocess.PIPE
        )
        time.sleep(2)  # Give load balancer time to start
        print("Load balancer started!")

    def connect_client(self):
        """Connect and initialize the client"""
        from client.client import KVClient
        print("Client connecting to the load balancer...")
        self.client = KVClient()
        print("Client connected!")

    def initialize_system(self):
        """Initialize the system by sending server list to load balancer"""
        print("Initializing the system...")
        response = self.client.kv_init()
        time.sleep(4)
        print(f"Initialization response: {response}")

    def run_test_operations(self):
        """Run some basic test operations"""
        print("\n--- Running test operations ---")
        
        # Test PUT operations
        print("\nPutting test data...")
        test_data = {
            "123": "test_value",            # 9004
            "pokemon": "pikachu",           # 9001
            "solo leveling": "dungeon",     # 
            "demon slayers": "Kokushibo",   #
            "tirth": "System"               # 9005
        }
        
        for key, value in test_data.items():
            status = self.client.kv_put(key, value)
            print(f"PUT {key}={value}: status={status}")
            
        # Test GET operations
        print("\nGetting test data...")
        for key in test_data.keys():
            value, status = self.client.kv_get(key)
            print(f"GET {key}: value={value}, status={status}")


        print("\nTesting Duplicate entry")
        key, value = "tirth", "System"
        status = self.client.kv_put(key, value)
        print(f"PUT {key}: value={value}, status={status}")


        value, status = self.client.kv_get(key)
        print(f"GET {key}: value={value}, status={status}")
            
        # Test non-existent key
        print("\nTesting non-existent key:")
        value, status = self.client.kv_get("non_existent_key")
        print(f"GET non_existent_key: value={value}, status={status}")
        time.sleep(2)

    def run_server_failure_scenerio(self):
        # self.client = None
        # from client.client import KVClient
        # self.client = KVClient()

        print("\n--- Running server failure scenerio ---")
        subprocess.run(['docker-compose', 'stop', 'kvstore1'])
        subprocess.run(['docker-compose', 'stop', 'kvstore3'])
        time.sleep(5)

        key, value = "123", "luffy"     # maps to kvstore4: next 2 nodes kvstore1, kvstore3 which are down
        status = self.client.kv_put(key, value)
        time.sleep(2)
        print(f"PUT {key}:{value} status={status}")

        value, status = self.client.kv_get(key)
        time.sleep(2)
        print(f"GET {key}: value={value}, status={status}")

        print("\nStarting the servers...")
        subprocess.run(['docker-compose', 'start', 'kvstore1'])
        subprocess.run(['docker-compose', 'start', 'kvstore3'])
        time.sleep(5)

        value, status = self.client.kv_get(key)
        print(f"GET {key}: value={value}, status={status}")


    def cleanup(self):
        """Clean up all resources"""
        print("\nCleaning up...")
                
        if self.lb_process:
            self.lb_process.terminate()
            
        # subprocess.run(['docker-compose', 'down'])
        print("Cleanup complete!")

    def run_all(self):
        """Run the full automation sequence"""
        try:
            self.setup_environment()
            self.start_docker_containers()
            self.start_load_balancer()
            self.connect_client()
            self.initialize_system()
            self.run_test_operations()
            self.run_server_failure_scenerio()
            input("\nPress Enter to exit and cleanup...")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.cleanup()

if __name__ == "__main__":
    automation = KVStoreAutomation()
    
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\nInterrupted! Cleaning up...")
        automation.cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    automation.run_all()