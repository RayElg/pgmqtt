FROM python:3.12-slim
RUN pip install --no-cache-dir flask psycopg2-binary
COPY server.py .
CMD ["python", "-u", "server.py"]
