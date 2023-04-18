sbt compile
docker exec spark-master spark-submit
 --master spark://spark-master:7077  \
 --deploy-mode client \
 /verve-group_2.13-0.0.1.jar

