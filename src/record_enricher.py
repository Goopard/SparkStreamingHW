from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from py4j.java_gateway import JavaGateway

from monitoring_record import MonitoringRecord
from kafka_senders import send_file_to_kafka, send_stream_to_kafka


BROKER = 'sandbox-hdp.hortonworks.com:6667'


def send_to_network(key, value):
    """This function sends a raw monitoring record to a Java Gateway and receives the enriched reply as a json.

    :param key: Key of the input monitoring record.
    :type key: str.
    :param value: Value of the input monitoring record.
    :type value: str.
    :return: json -- the required enriched monitoring record in a json format.
    """
    gateway = JavaGateway()
    values = MonitoringRecord(value).to_list()
    raw_record = gateway.jvm.com.epam.bcc.htm.MonitoringRecord(*values)
    record = gateway.entry_point.mappingFunc(key, raw_record).toJson()
    return record


def receive(ssc, topic, broker):
    """This function is used to create a DStream populated from a given Kafka topic.

    :param ssc: StreamingContext to use.
    :type ssc: StreamingContext.
    :param topic: Input Kafka topic.
    :type topic: str.
    :param broker: Input Kafka broker.
    :type broker: str.
    :return: DStream
    """
    stream = KafkaUtils.createDirectStream(ssc, [topic],
                                           kafkaParams={'bootstrap.servers': broker, 'auto.offset.reset': 'smallest'})
    return stream


def process_stream(stream):
    """This function processes an input stream of data through the HTM network, enriching the records.

    :param stream: Input data stream of raw monitoring records.
    :type stream: DStream.
    :return: DStream -- output data stream of enriched monitoring records.
    """
    return stream.map(lambda record: send_to_network(record[0], record[1])).mapValues(MonitoringRecord.from_json)


if __name__ == '__main__':
    conf = SparkConf().setMaster('local[2]').setAppName('test')
    conf.set('spark.submit.pyFiles', ['/usr/HW/monitoring_record.py', '/usr/HW/kafka_senders.py'])
    sc = SparkContext(conf=conf)
    sc.addPyFile('/usr/HW/monitoring_record.py')
    sc.addPyFile('/usr/HW/kafka_senders.py')
    ssc = StreamingContext(sc, batchDuration=5)

    send_file_to_kafka('/usr/datasets/records_short.csv', 'monitoring1', BROKER)

    raw_records = receive(ssc, 'monitoring1', BROKER)
    enriched_records = process_stream(raw_records)
    send_stream_to_kafka(enriched_records, 'monitoringEnriched1', BROKER)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
