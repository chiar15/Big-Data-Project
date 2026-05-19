/opt/bitnami/spark/bin/spark-submit \
  --class it.unisa.hpc.worldcup.WorldCupDriver \
  --master local \
  ./Spark.jar \
  ./input ./outputSingle
