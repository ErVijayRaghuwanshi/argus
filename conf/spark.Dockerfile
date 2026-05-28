# ==============================================================================
# Project Argus: Hardened Spark 4.1.2 Image with Declarative Pipeline (SDP) Support
# ==============================================================================
FROM apache/spark:4.1.2

# Switch to root to perform system package and Python installations
USER root

# Install required Python packages for PySpark Declarative Pipelines (SDP) and Spark Connect
RUN pip3 install --no-cache-dir \
    "pyyaml" \
    "pandas>=2.2.0" \
    "pyarrow>=15.0.0" \
    "grpcio>=1.48.1" \
    "protobuf<7.0.0" \
    "grpcio-status>=1.48.1" \
    "zstandard>=0.25.0"

# Copy Spark defaults configuration for packages and caching
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Switch back to the non-root spark user for security compliance
USER spark
