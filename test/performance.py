import os
import sys
import time
import random
import argparse
import numpy as np
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

# Add the parent directory to sys.path to import client.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from client.client import KVClient

path = os.path.dirname(os.path.abspath(__file__))

class PerformanceTester:
    def __init__(self, client_count=1, key_count=100, value_size=100):
        """
        Initialize performance tester with configuration parameters.
        
        Args:
            client_count: Number of concurrent clients to simulate            
            key_count: Number of unique keys to use in the test
            value_size: Size of values in bytes
        """
        self.client_count = client_count
        self.key_count = key_count
        self.value_size = value_size
        self.clients = []
        self.results = {
            'uniform': {'get': [], 'put': []},
            'hotspot': {'get': [], 'put': []}
        }
        self.throughput_results = {
            'uniform': {'get': 0, 'put': 0},
            'hotspot': {'get': 0, 'put': 0}
        }
        
        # Prepare test data
        self.keys = [f"key_{i}" for i in range(self.key_count)]
        self.values = [f"value_{'A' * (self.value_size - 6)}_{i}" for i in range(self.key_count)]
        
        # Initialize clients
        self._init_clients()
        
    def _init_clients(self):
        """Initialize KVClient instances"""
        print("Initializing clients...")
        for _ in range(self.client_count):
            try:
                client = KVClient()
                print(f"Client connected to load balancer")
                self.clients.append(client)
            except Exception as e:
                print(f"Error initializing client: {e}")
        
        # Initialize the system once
        if self.clients:
            self.clients[0].kv_init()
            print(f"Successfully initialized {len(self.clients)} clients")
        else:
            print("Failed to initialize any clients. Exiting.")
            exit(1)

    def _generate_uniform_workload(self, op_count):
        """Generate a uniform distribution workload"""
        ops = []
        for _ in range(op_count):
            key_index = random.randint(0, self.key_count - 1)
            ops.append(self.keys[key_index])
        return ops
        
    def _generate_hotspot_workload(self, op_count, hotspot_keys=5, hotspot_prob=0.9):
        """Generate a hotspot distribution where a few keys are accessed frequently"""
        hotspot_indices = random.sample(range(self.key_count), hotspot_keys)
        ops = []
        for _ in range(op_count):
            if random.random() < hotspot_prob:
                # Select from hotspot keys
                key_index = random.choice(hotspot_indices)
            else:
                # Select from all other keys
                key_index = random.randint(0, self.key_count - 1)
                while key_index in hotspot_indices:
                    key_index = random.randint(0, self.key_count - 1)
            ops.append(self.keys[key_index])
        return ops
        
    def _perform_put(self, client, key, value):
        """Perform a PUT operation and measure latency"""
        start_time = time.time()
        status = client.kv_put(key, value)
        end_time = time.time()
        latency = (end_time - start_time) * 1000  # converting to ms
        return {'latency': latency, 'status': status}
        
    def _perform_get(self, client, key):
        """Perform a GET operation and measure latency"""
        start_time = time.time()
        value, status = client.kv_get(key)
        end_time = time.time()
        latency = (end_time - start_time) * 1000  # converting to ms
        return {'latency': latency, 'status': status, 'value': value}
        
    def _worker_put(self, args):
        """Worker function for PUT operations in thread pool"""
        client_idx, key, value = args
        if client_idx < len(self.clients):
            client = self.clients[client_idx]
            return self._perform_put(client, key, value)
        return {'latency': 0, 'status': -1}
        
    def _worker_get(self, args):
        """Worker function for GET operations in thread pool"""
        client_idx, key = args
        if client_idx < len(self.clients):
            client = self.clients[client_idx]
            return self._perform_get(client, key)
        return {'latency': 0, 'status': -1, 'value': None}
    
    def _run_workload(self, distribution, op_type, total_ops=1000):
        """
        Run a specific workload and measure performance
        
        Args:
            distribution: Which distribution to use ('uniform', 'hotspot')
            op_type: Operation type ('get' or 'put')
            total_ops: Total operations to perform
        """
        print(f"Running {distribution} distribution {op_type} workload...")
        
        # Generate workload based on distribution
        if distribution == 'uniform':
            workload = self._generate_uniform_workload(total_ops)
        elif distribution == 'hotspot':
            workload = self._generate_hotspot_workload(total_ops)
        else:
            print(f"Unknown distribution: {distribution}")
            return
            
        # Prepare tasks for thread pool
        tasks = []
        for i, key in enumerate(workload):
            client_idx = i % len(self.clients)
            if op_type == 'put':
                value = self.values[self.keys.index(key)]
                tasks.append((client_idx, key, value))
            else:  # get
                tasks.append((client_idx, key))
                
        # Execute operations in parallel
        latencies = []
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=self.client_count) as executor:
            if op_type == 'put':
                for result in executor.map(self._worker_put, tasks):
                    if result['status'] != -1:
                        latencies.append(result['latency'])
            else:
                for result in executor.map(self._worker_get, tasks):
                    if result['status'] != -1:
                        latencies.append(result['latency'])
        
        end_time = time.time()
        
        # Calculating throughput
        elapsed = end_time - start_time
        if elapsed > 0:
            throughput = len(latencies) / elapsed
        else:
            throughput = 0
            
        # Store results
        self.results[distribution][op_type] = latencies
        self.throughput_results[distribution][op_type] = throughput
        
        print(f"{distribution.capitalize()} {op_type.upper()} workload completed:")
        print(f"  Ops completed: {len(latencies)}")
        print(f"  Avg latency: {np.mean(latencies):.2f} ms")
        print(f"  99th percentile: {np.percentile(latencies, 99):.2f} ms")
        print(f"  Throughput: {throughput:.2f} ops/sec")
        print("")
        
    def prepare_data(self):
        """Prepare initial data by performing PUT operations"""
        print("Preparing initial data...")
        
        # Generate uniform workload for all keys
        workload = [(i % len(self.clients), self.keys[i], self.values[i]) 
                    for i in range(self.key_count)]
        
        with ThreadPoolExecutor(max_workers=self.client_count) as executor:
            results = list(executor.map(self._worker_put, workload))
            
        success_count = sum(1 for r in results if r['status'] != -1)
        print(f"Initial data preparation completed: {success_count}/{self.key_count} keys stored")
        
    def run_tests(self, ops_per_test=1000):
        """Run all performance tests"""
        # First prepare data
        self.prepare_data()
        
        # Test different distributions and operations
        for distribution in ['uniform', 'hotspot']:
            self._run_workload(distribution, 'put', ops_per_test)
            self._run_workload(distribution, 'get', ops_per_test)
                
    def generate_report(self, output_dir=path+"/results"):
        """Generate performance report with charts"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        plt.figure(figsize=(12, 8))
        
        # GET latency comparison
        plt.subplot(2, 2, 1)
        for dist in ['uniform', 'hotspot']:
            if self.results[dist]['get']:
                plt.hist(self.results[dist]['get'], alpha=0.5, bins=20, label=dist)
        plt.title('GET Latency Distribution')
        plt.xlabel('Latency (ms)')
        plt.ylabel('Count')
        plt.legend()
        
        # PUT latency comparison
        plt.subplot(2, 2, 2)
        for dist in ['uniform', 'hotspot']:
            if self.results[dist]['put']:
                plt.hist(self.results[dist]['put'], alpha=0.5, bins=20, label=dist)
        plt.title('PUT Latency Distribution')
        plt.xlabel('Latency (ms)')
        plt.ylabel('Count')
        plt.legend()
        
        # Throughput comparison
        plt.subplot(2, 2, 3)
        distributions = list(self.throughput_results.keys())
        get_throughput = [self.throughput_results[dist]['get'] for dist in distributions]
        put_throughput = [self.throughput_results[dist]['put'] for dist in distributions]
        
        x = range(len(distributions))
        width = 0.35
        
        plt.bar([i - width/2 for i in x], get_throughput, width, label='GET')
        plt.bar([i + width/2 for i in x], put_throughput, width, label='PUT')
        plt.xticks(x, distributions)
        plt.title('Throughput Comparison')
        plt.ylabel('Operations per second')
        plt.legend()
        
        # CDF of latencies
        plt.subplot(2, 2, 4)
        for dist in ['uniform', 'hotspot']:
            if self.results[dist]['get']:
                data = sorted(self.results[dist]['get'])
                n = len(data)
                y = np.arange(1, n+1) / n
                plt.plot(data, y, marker='.', linestyle='none', label=f'{dist} GET')
            if self.results[dist]['put']:
                data = sorted(self.results[dist]['put'])
                n = len(data)
                y = np.arange(1, n+1) / n
                plt.plot(data, y, marker='.', linestyle='none', label=f'{dist} PUT')
        plt.title('Latency CDF')
        plt.xlabel('Latency (ms)')
        plt.ylabel('CDF')
        plt.legend()
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/performance_charts.png')
        
        # Create summary table
        summary_data = []
        for dist in ['uniform', 'hotspot']:
            for op_type in ['get', 'put']:
                if self.results[dist][op_type]:
                    latencies = self.results[dist][op_type]
                    summary_data.append({
                        'Distribution': dist,
                        'Operation': op_type,
                        'Count': len(latencies),
                        'Avg Latency (ms)': np.mean(latencies),
                        'Min (ms)': np.min(latencies),
                        'Max (ms)': np.max(latencies),
                        'Median (ms)': np.median(latencies),
                        'P99 (ms)': np.percentile(latencies, 99),
                        'Throughput (ops/sec)': self.throughput_results[dist][op_type]
                    })
        

        if summary_data:
            # Generating markdown file
            df = pd.DataFrame(summary_data)
            with open(f'{output_dir}/performance_summary.md', 'w') as f:
                f.write(df.to_markdown())
                
            print(f"Performance report generated in {output_dir}")
        else:
            print("No performance data to report")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='KV Store Performance Tester')
    parser.add_argument('--clients', type=int, default=1, help='Number of concurrent clients')    
    parser.add_argument('--keys', type=int, default=1000, help='Number of unique keys')
    parser.add_argument('--value-size', type=int, default=100, help='Size of values in bytes')
    parser.add_argument('--ops', type=int, default=1000, help='Operations per test')
    args = parser.parse_args()
    
    print(f"Starting performance test with {args.clients} clients, {args.keys} keys, {args.ops} ops per test")
    
    tester = PerformanceTester(
        client_count=args.clients,        
        key_count=args.keys,
        value_size=args.value_size
    )
    
    try:
        tester.run_tests(ops_per_test=args.ops)
        tester.generate_report()
    except Exception as e:
        print(f"Error during test execution: {e}")
