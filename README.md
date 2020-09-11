
# Iot-and-Big-Data-Application-using-aws-and-apache-kafka

### Internet of things , Big Data Analytics using Aws-kafka, spark and other aws services.

![image](https://user-images.githubusercontent.com/48589838/77175766-22ae6480-6ae9-11ea-81c7-365aa49856cb.png)



![image](https://user-images.githubusercontent.com/48589838/77175943-61dcb580-6ae9-11ea-87ff-c41302b153c1.png)




 To run spark job on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart
 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iot-sql.py \
      localhost:9092 test`

## Steps

 1.Define function to process RDDs of the json DStream to convert them to DataFrame and run SQL queries
    

 2.Process each RDD of the DStream coming in from Kafka


 3.Set number of simulated messages to generate


 4.Generate JSON output
