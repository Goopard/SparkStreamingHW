from kafka import KafkaProducer

from contextlib import contextmanager

from monitoring_record import MonitoringRecord


@contextmanager
def context_manager_producer(**kwargs):
    producer = KafkaProducer(**kwargs)
    yield producer
    producer.flush()
    producer.close()


def send_file_to_kafka(path, topic, broker):
    """This function reads the file from the local file system and sends all of its contents to the given Kafka topic.

    :param path: Path to the input file.
    :type path: str.
    :param topic: Kafka topic.
    :type topic: str.
    :param broker: Kafka broker to use.
    :type broker: str.
    :return:
    """
    with open(path, 'r') as file, context_manager_producer(bootstrap_servers=broker, key_serializer=serializer,
                                                           value_serializer=serializer) as producer:
        contents = file.read().splitlines()
        for line in contents:
            if line:
                record = MonitoringRecord(*line.split(','))
                producer.send(topic, key=record.get_key(), value=record)


def serializer(record):
    """This function is used to serialize some given object to a bytes object in order to pass this object to a kafka
    topic later.

    :param record: Some given input object.
    :type record: any.
    :return: bytes.
    """
    return str(record).encode()


def send_record_to_kafka(record, topic, broker):
    """This function sends a record to the given Kafka topic.

    :param record: Record to send.
    :type record: MonitoringRecord or tuple.
    :param topic: Kafka topic.
    :type topic: str.
    :param broker: Kafka broker to use.
    :type broker: str.
    """
    with context_manager_producer(bootstrap_servers=broker, key_serializer=serializer, value_serializer=serializer) \
            as producer:
        producer.send(topic, key=record.get_key(), value=record)


def send_stream_to_kafka(stream, topic, broker):
    """This function sends the enriched stream data to the output Kafka topic.

    :param topic: Output Kafka topic.
    :type topic: str.
    :param stream: Enriched data stream.
    :type stream: DStream.
    :param broker: Kafka broker to use.
    :type broker: str.
    """
    def send_partition_to_kafka(partition):
        for record in partition:
            send_record_to_kafka(record, topic, broker)

    stream.foreachRDD(lambda rdd: rdd.foreachPartition(send_partition_to_kafka))
