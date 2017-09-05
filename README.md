# another-kafka-consumer

## Feature

1. Get the latest offset
1. Fetch the last N messages before the latest offset

## Usage

```bash
python another-kafka-consumer.py --topic test --group test --brokers "127.0.0.1:9092" --check
python another-kafka-consumer.py --topic test --group test --brokers "127.0.0.1:9092" --fetch --num 10000
```