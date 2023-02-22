import findspark
findspark.init("E:\App\Spark_hadoop\spark-2.3.2-bin-hadoop2.7")
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SQLContext
import json
import sys
from os import environ
from kafka import KafkaProducer
from configparser import ConfigParser


def set_global(config):
    globals()['dashboard_topic_name'] = config['Resources']['dashboard_topic_name']


def sum_tags(new_values, last_sum):
    if last_sum is None:
        return sum(new_values)
    return sum(new_values) + last_sum


def getSparkSessionInstance(spark_context):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SQLContext(spark_context)
    return globals()['sparkSessionSingletonInstance']


def getKafkaInstance():
    if ('kafkaSingletonInstance' not in globals()):
        globals()['kafkaSingletonInstance'] = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                                            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return globals()['kafkaSingletonInstance']


def process_hashtags(time, rdd):
    try:
        spark_sql = getSparkSessionInstance(rdd.context)
        rowRdd = rdd.map(lambda tag: Row(hashtag=tag[0], frequency=tag[1]))
        hashtagsDataFrame = spark_sql.createDataFrame(rowRdd)
        hashtagsDataFrame.createOrReplaceTempView("hashtags")
        hashtagCountsDataFrame = spark_sql.sql( 
            "select hashtag, frequency from hashtags order by frequency desc limit 10")
        hashtagCountsDataFrame.show()

        send_to_kafka(hashtagCountsDataFrame)

    except:
        e = sys.exc_info()[0]
        print(e)


def send_to_kafka(hashtagCountsDataFrame):

    top_hashtags = {}

    for hashtag, frequency in hashtagCountsDataFrame.collect():
        top_hashtags[hashtag] = frequency

    # print("Trending HashTags = ", top_hashtags)

    producer = getKafkaInstance()
    producer.send(globals()['dashboard_topic_name'], value=top_hashtags)


if __name__ == "__main__":

    config = ConfigParser()
    config.read(r"conf\app.conf")
    set_global(config)
    pyspark_environ = config['Resources']['pyspark_environ']
    
    environ['PYSPARK_SUBMIT_ARGS'] = pyspark_environ

    sparkConf = SparkConf("TwitterDataAnalysis")

    sparkConf.setMaster("local[2]")

    sc = SparkContext(conf=sparkConf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60)
    ssc.checkpoint("checkpointTwitterApp")

    bootstap_server = config['Kafka_param']['bootstrap.servers']
    zookeeper = config['Kafka_param']['zookeeper.connect']
    group_id = config['Kafka_param']['group.id']
    timeout = config['Kafka_param']['zookeeper.connection.timeout.ms']

    kafkaParam = {
        "zookeeper.connect": zookeeper,
        "group.id": group_id,
        "zookeeper.connection.timeout.ms": timeout,
        "bootstrap.servers": bootstap_server
    }

    tweets = KafkaUtils.createDirectStream(
        ssc, [config['Resources']['app_topic_name']], kafkaParams=kafkaParam, valueDecoder=lambda x: json.loads(x.decode('utf-8')))

    words = tweets.map(lambda v: v[1]["text"]).flatMap(lambda t: t.split(" "))

    hashtags = words.filter(lambda tag: len(
        tag) > 2 and '#' == tag[0]).countByValue().updateStateByKey(sum_tags)

    hashtags.foreachRDD(process_hashtags)

    ssc.start()
    ssc.awaitTermination() 
