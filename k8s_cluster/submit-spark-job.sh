#!/bin/bash

# Configuration - Update these values
K8S_MASTER="k8s://5E17437E68F5ED264BD1CF0F2D74ABCC.gr7.ap-south-1.eks.amazonaws.com"  # Replace with your API server
IMAGE_NAME="902190101181.dkr.ecr.ap-south-1.amazonaws.com/pyspark-k8s-demo"    # Replace with your image
NAMESPACE="spark-jobs"
echo "🚀 Submitting PySpark job to Kubernetes..."
echo "K8s Master: $K8S_MASTER"
echo "Image: $IMAGE_NAME"
echo "Namespace: $NAMESPACE"

./spark-submit \
  --master $K8S_MASTER \
  --deploy-mode cluster \
  --name pyspark-k8s-demo \
  --conf spark.executor.instances=2 \
  --conf spark.executor.memory=1g \
  --conf spark.executor.cores=1 \
  --conf spark.driver.memory=1g \
  --conf spark.driver.cores=1 \
  --conf spark.kubernetes.container.image=$IMAGE_NAME \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-service-account \
  --conf spark.kubernetes.namespace=$NAMESPACE \
  --conf spark.kubernetes.driver.pod.name=pyspark-driver-$(date +%s) \
  --conf spark.kubernetes.executor.deleteOnTermination=true \
  --conf spark.kubernetes.driver.label.app=pyspark-demo \
  --conf spark.kubernetes.executor.label.app=pyspark-demo \
  --conf spark.ui.port=4040 \
  --conf spark.kubernetes.driver.annotation.cluster-autoscaler.kubernetes.io/safe-to-evict=false \
  local:///opt/spark/work-dir/pyspark_k8s_demo.py

echo "✅ Job submitted! Check the logs with:"
echo "kubectl logs -f -n $NAMESPACE <driver-pod-name>"