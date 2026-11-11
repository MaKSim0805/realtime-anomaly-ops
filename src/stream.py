"""Score Kafka telemetry using a rolling window per metric."""
import json
import logging
import math
import os
from collections import defaultdict, deque
from dataclasses import asdict

from .detector import detect

INPUT_TOPIC = "telemetry"
ALERT_TOPIC = "alerts"


class Scorer:
    def __init__(self):
        self.windows = defaultdict(lambda: deque(maxlen=30))

    def score(self, payload):
        event = json.loads(payload)
        metric = event["metric"]
        value = event["value"]
        if not isinstance(metric, str) or not metric or len(metric) > 128:
            raise ValueError("Invalid metric name")
        if type(value) not in (int, float) or not math.isfinite(value) or abs(value) > 1e12:
            raise ValueError("Invalid metric value")
        window = self.windows[metric]
        if len(window) >= 5:
            decision = detect(value, list(window))
            if decision.alert:
                return {"metric": metric, "value": value, **asdict(decision)}
        window.append(value)
        return None


def main():
    from confluent_kafka import Consumer, KafkaException, Producer

    logging.basicConfig(level=logging.INFO)
    broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": "anomaly-scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": broker, "acks": "all", "message.timeout.ms": 10000})
    scorer = Scorer()
    consumer.subscribe([INPUT_TOPIC])
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                alert = scorer.score(message.value())
            except (ValueError, KeyError, TypeError, OverflowError) as exc:
                logging.warning("Skipping invalid event at offset %s: %s", message.offset(), exc)
                alert = None
            if alert:
                source = f"{message.topic()}:{message.partition()}:{message.offset()}"
                errors = []
                producer.produce(
                    ALERT_TOPIC,
                    key=source,
                    value=json.dumps({**alert, "source": source}),
                    on_delivery=lambda error, _: errors.append(error) if error else None,
                )
                if producer.flush(12) or errors:
                    raise RuntimeError(f"Alert delivery failed at {source}: {errors}")
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
