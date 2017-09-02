# coding=utf-8

"""
A simple wrapper for kafka consumer.

Authors: secfree(zzd7zzd@gmail.com)
"""

import argparse
import kafka


def parse_arguments():
    parser = argparse.ArgumentParser(description='another kafka consumer')
    parser.add_argument("num",
                        help='number of messages read from each partition')
    parser.add_argument('--topic', help='kafka topic name')
    parser.add_argument('--group', help='kafka group id')
    parser.add_argument('--brokers', help='kafka brokers')
    return parser.parse_args()


def main():
    args = parse_arguments()
    # Get the num
    n = int(args.num)

    # Connect to kafka
    consumer = kafka.KafkaConsumer(
        args.topic, group_id=args.group, bootstrap_servers=args.brokers)
    # Make partition assigned
    consumer.poll()
    # Get partition info
    partitions = consumer.partitions_for_topic(args.topic)

    topic_ps = []
    for i in partitions:
        topic_ps.append(kafka.TopicPartition(args.topic, i))

    # Get the latest offset for each partition
    pos = []
    for tp in topic_ps:
        pos.append(consumer.position(tp))

    # Shift position
    for i in xrange(len(pos)):
        if pos[i] > n:
            pos[i] -= n
        else:
            pos[i] = 0

    # Seek
    for i in xrange(len(pos)):
        consumer.seek(topic_ps[i], pos[i])

    # Read the messages
    for msg in consumer:
        print msg.value


if __name__ == '__main__':
    main()
