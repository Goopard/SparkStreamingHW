from kafka_senders import send_file_to_kafka

INPUT_RECORDS_PATH = '/usr/datasets/records.csv'
TOPIC = 'monitoring3'
BROKER = 'sandbox-hdp.hortonworks.com:6667'

if __name__ == '__main__':
    send_file_to_kafka(INPUT_RECORDS_PATH, TOPIC, BROKER)
