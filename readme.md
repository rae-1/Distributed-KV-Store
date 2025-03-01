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

Now to setup the server build the image and run it using:
```bash
docker build -t server_1
docker run -it --name=server_1_con -p 9000:9000 server_1
```

To keep track of what is happening to the server, client, and LB you can look at .log files.


If you want to setup a custom network and use it:
```bash
docker network create -d bridge --subnet=192.168.1.0/24 kv_store
docker run -it --name=server_1_con --net=kv_store --ip=192.168.1.100 -p 9000:9000 server_1
```