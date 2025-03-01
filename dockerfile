FROM python:3

WORKDIR /app

COPY server/server.py .
COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "server.py"]