## Automated tests:
Execute the run_kv_store.py, it includes the PUT and GET calls during normal operations as well as when few of the servers are down. From the root directory run

```bash
python3 run_kv_store.py
```

## Automated tests:
Execute the run_kv_store.py, it includes the PUT and GET calls during normal operations as well as when few of the servers are down. From the root directory run

```bash
python3 run_kv_store.py
```

## Instructions:
Make sure you are in the root directory. Now, create a virtual environment, activate it, and install the dependencies.

```bash
source .venv/bin/activate
pip3 install -r requirements.txt
```

Run the load balancer in one terminal and the client on the other.
```bash
python3 loadBalancer/consistentHashing.py

# open another terminal
python3 client/client.py
```

Now to setup the servers, use docker-compose to build the images and run it using:
Now to setup the servers, use docker-compose to build the images and run it using:
```bash
docker-compose build
docker-compose up -d
docker-compose build
docker-compose up -d
```
If the port 9000 is already in use then change it to whichever is available and change _translate_address functions a bit.

Now, choose one of the operations from the client side. Make sure that the servers are up before doing this.
If the port 9000 is already in use then change it to whichever is available and change _translate_address functions a bit.

Now, choose one of the operations from the client side. Make sure that the servers are up before doing this.

To keep track of what is happening to the server, client, and LB you can look at .log files.
