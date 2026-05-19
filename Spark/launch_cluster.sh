/opt/bitnami/spark/bin/spark-submit \
  --class it.unisa.hpc.worldcup.WorldCupDriver \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --supervise \
  --executor-memory 1G \
  ./Spark.jar \
./input ./outputCluster
