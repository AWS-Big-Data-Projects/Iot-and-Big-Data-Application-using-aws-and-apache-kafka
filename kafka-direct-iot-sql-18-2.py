"""
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iot-sql.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys
import re
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")

    ##############
    # Globals
    ##############
    globals()['maxTemp'] = sc.accumulator(0.0)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    jsonDStream = kvs.map(lambda (key, value): value)

    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def process(time, rdd):
        # Match local function variables to global variables
        maxTemp = globals()['maxTemp']

        print("========= %s =========" % str(time))
        print("rdd = %s" % str(rdd))

        try:
            # Parse the one line JSON string from the DStream RDD
            jsonString = rdd.map(lambda x: \
                re.sub(r"\s+", "", x, flags=re.UNICODE)).reduce(add)
            print("jsonString = %s" % str(jsonString))

            # Convert the JSON string to an RDD
            jsonRDDString = sc.parallelize([str(jsonString)])

            # Convert the JSON RDD to Spark SQL Context
            jsonRDD = sqlContext.read.json(jsonRDDString)

            # Register the JSON SQL Context as a temporary SQL Table
            print("JSON Schema\n=====")
            jsonRDD.printSchema()
            jsonRDD.registerTempTable("iotmsgsTable")

            #################################
            # Processing and Analytics go here
            ##################################

            print("ST21 PARAMETRICS FROM MULTIPLE TOOLS AND LOCATIONS.\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.pwr, \
                payload.data.volt, payload.data.bias, payload.data.flow, payload.data.pres, \
                payload.data.temp from iotmsgsTable order by index").show(n=100)

            #### LIMITS
            pwrHiLim = 0.206; pwrLoLim = 0.193
	    voltHiLim = 277.6; voltLoLim = 273.8
            biasHiLim = 103.0; biasLoLim = 97.2
            flowHiLim = 40.7; flowLoLim = 39.3
            presHiLim = 12.8; presLoLim = 7.8
            tempHiLim = 81.5; tempLoLim = 78.7
            

            print("#### PWR #############################################################")
            print("SEARCH OUT OF LIMIT PWR >", pwrHiLim, " OR <", pwrLoLim,  "\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.pwr from iotmsgsTable \
              where payload.data.pwr > 0.206").show(n=20)

            print("ALERT: IF NOTED ABOVE, PWR HIGHER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.pwr from iotmsgsTable \
              where payload.data.pwr < 0.193").show(n=20)

            print("ALERT: IF NOTED ABOVE, PWR LOWER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n\n")

            print("#### VOLTAGE ###########################################################")
            print("SEARCH OUT OF LIMIT VOLTAGE >", voltHiLim, " OR <", voltLoLim,  "\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.volt from iotmsgsTable \
              where payload.data.volt > 277.6").show(n=20)

            print("ALERT: IF NOTED ABOVE, VOLTAGE HIGHER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.volt from iotmsgsTable \
              where payload.data.volt < 273.8").show(n=20)

            print("ALERT: IF NOTED ABOVE, VOLTAGE LOWER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n\n")
 
            print ("#### BIAS ###############################################################")
            print("SEARCH OUT OF LIMIT BIAS >", biasHiLim, " OR <", biasLoLim,  "\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.bias from iotmsgsTable \
              where payload.data.bias > 103.0").show(n=20)

            print("ALERT: IF NOTED ABOVE, BIAS HIGHER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.bias from iotmsgsTable \
              where payload.data.bias < 97.2").show(n=20)

            print("ALERT: IF NOTED ABOVE, BIAS LOWER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n\n")
 
            print("### Ar FLOW ###############################################################")
            print("SEARCH OUT OF LIMIT AR FLOW >", flowHiLim, " OR <", flowLoLim,  "\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.flow from iotmsgsTable \
              where payload.data.flow > 40.7").show(n=20)

            print("ALERT: IF NOTED ABOVE, AR FLOW HIGHER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.flow from iotmsgsTable \
              where payload.data.flow < 39.3").show(n=20)

            print("ALERT: IF NOTED ABOVE, AR FLOW LOWER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n\n")            

 	    print ("#### PRESSURE #############################################################")
            print("SEARCH OUT OF LIMIT PRESSURE >", presHiLim, " OR <", presLoLim,  "\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.pres from iotmsgsTable \
              where payload.data.pres > 12.8").show(n=20)

            print("ALERT: IF NOTED ABOVE, PRESSURE HIGHER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.pres from iotmsgsTable \
              where payload.data.pres < 7.8").show(n=20)

            print("ALERT: IF NOTED ABOVE, PRESSURE LOWER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n\n")	

            print("#### TEMP ##################################################################")
            print("SEARCH OUT OF LIMIT TEMP >", tempHiLim, " OR <", tempLoLim,  "\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.temp from iotmsgsTable \
              where payload.data.temp > 81.5").show(n=20)

            print("ALERT: IF NOTED ABOVE, TEMP HIGHER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n")

            sqlContext.sql("select index, time, loc, tool, stn, guid, slot, payload.data.temp from iotmsgsTable \
              where payload.data.temp < 78.7").show(n=20)

            print("ALERT: IF NOTED ABOVE, TEMP LOWER THAN LIMIT FOR DISKS IN ABOVE CASSETTES (guids) & SLOTS.\n\n")

            # Clean-up
            sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except:
            pass

    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()