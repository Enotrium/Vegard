FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    protobuf-compiler \
    grpcio-tools \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY pyproject.toml ./

# Install Python dependencies
RUN pip install --no-cache-dir -e .

# Copy application code
COPY syndar/ ./syndar/
COPY proto/ ./proto/
COPY configs/ ./configs/

# Generate proto stubs
RUN python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    proto/common.proto \
    proto/entity.proto \
    proto/drift.proto \
    proto/soil.proto \
    proto/task.proto \
    proto/transport.proto

RUN mv proto/*_pb2.py syndar/proto/ && \
    mv proto/*_pb2_grpc.py syndar/proto/

# Create directories for data
RUN mkdir -p /var/lib/syndar /var/log/syndar

# Expose ports
EXPOSE 50051 8000 1883

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run the application
CMD ["uvicorn", "syndar.command.api:app", "--host", "0.0.0.0", "--port", "8000"]
