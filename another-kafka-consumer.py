# coding=utf-8

"""
A simple wrapper for kafka consumer.

Authors: secfree(zzd7zzd@gmail.com)
"""

import argparse
import kafka


class Consumer(object):
    def __init__(self, topic, group, brokers):
        """
        :param topic: str, kafka topic
        :param group: str, kafka group_id
        :param brokers: str, kafka broker list
        """
        # Connect to kafka
        self._topic = topic
        self._consumer = kafka.KafkaConsumer(
            topic, group_id=group, bootstrap_servers=brokers)
        self._partitions = None
        self._topic_ps = []
        self._pos = []

    def check(self):
        """
        Get the latest offset
        :return: 
        """
        # Make partition assigned
        self._consumer.poll()
        # Get partition info
        self._partitions = self._consumer.partitions_for_topic(self._topic)

        for i in self._partitions:
            self._topic_ps.append(kafka.TopicPartition(self._topic, i))

        # Get the latest offset for each partition
        for tp in self._topic_ps:
            self._pos.append(self._consumer.position(tp))

        print 'Offset list: ', self._pos
        print 'Offset sum: ', sum(self._pos)

    def fetch(self, num):
        """
        ﻿Fetch the last N messages before the latest offset
        :param num: int, size
        :return: 
        """
        # Init
        self.check()

        # Shift position
        for i in xrange(len(self._pos)):
            if self._pos[i] > num:
                self._pos[i] -= num
            else:
                self._pos[i] = 0

        # Seek
        for i in xrange(len(self._pos)):
            self._consumer.seek(self._topic_ps[i], self._pos[i])

        # Read the messages
        for msg in self._consumer:
            print msg.value


def parse_arguments():
    parser = argparse.ArgumentParser(description='another kafka consumer')
    parser.add_argument('--check', action='store_true',
                        help='check latest offset')
    parser.add_argument('--fetch', action='store_true',
                        help='fetch the last N messages '
                             'before the latest offset')
    parser.add_argument('--num',
                        help='number of messages read from each partition')
    parser.add_argument('--topic', help='kafka topic name')
    parser.add_argument('--group', help='kafka group id')
    parser.add_argument('--brokers', help='kafka brokers')
    return parser.parse_args()


def main():
    args = parse_arguments()
    consumer = Consumer(topic=args.topic,
                        group=args.group,
                        brokers=args.brokers)
    if args.check:
        consumer.check()
    if args.fetch:
        consumer.fetch(int(args.num))


if __name__ == '__main__':
    main()
