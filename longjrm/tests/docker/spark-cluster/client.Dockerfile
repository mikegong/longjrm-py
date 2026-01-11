FROM python:3.10-slim-bullseye

# Install Java (required for PySpark) and basic tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install dependencies needed for our tests
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    DBUtils>=3.0.3

# Set working directory
WORKDIR /workspace/longjrm-py

# Keep container alive
CMD ["tail", "-f", "/dev/null"]
