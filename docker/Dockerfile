FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY proto ./proto
COPY minikv ./minikv
RUN pip install --no-cache-dir ".[test]" grpcio grpcio-tools
CMD ["python", "-m", "minikv.server.main"]
