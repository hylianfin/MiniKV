FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY proto ./proto
COPY minikv ./minikv
RUN pip install --no-cache-dir ".[test]" grpcio grpcio-tools \
 && python proto/gen_proto.py
EXPOSE 7001
CMD ["minikv", "server", "--help"]
