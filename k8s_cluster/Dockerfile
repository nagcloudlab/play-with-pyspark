# Use official Spark image with Python support
FROM apache/spark-py:v3.4.0

# Set working directory
WORKDIR /opt/spark/work-dir

# Copy your PySpark application
COPY pyspark_k8s_demo.py /opt/spark/work-dir/
COPY requirements.txt /opt/spark/work-dir/

# Install additional Python packages if needed
USER root
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to spark user
USER ${spark_uid}

# Set entrypoint
ENTRYPOINT ["/opt/entrypoint.sh"]