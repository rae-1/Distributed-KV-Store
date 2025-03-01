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

To keep track of what is happening to the server you can look at LB.log.