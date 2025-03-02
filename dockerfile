FROM python:3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update -y
RUN apt-get install -y iputils-ping
RUN apt install net-tools -y

COPY server/server.py .

CMD ["python", "server.py"]