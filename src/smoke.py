"""Send a synthetic incident through the running Compose stack."""
import json
import os
from time import monotonic
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from .stream import ALERT_TOPIC, INPUT_TOPIC


def main():
    broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    metric = f"checkout_latency_{uuid4().hex}"
    producer = Producer({"bootstrap.servers": broker, "message.timeout.ms": 10000})
    errors = []

    def record_delivery(error, _message):
        if error:
            errors.append(str(error))

    for value in (120, 130, 110, 125, 135, 128, 980):
        producer.produce(
            INPUT_TOPIC,
            key=metric,
            value=json.dumps({"metric": metric, "value": value}),
            on_delivery=record_delivery,
        )
    if producer.flush(12) or errors:
        raise RuntimeError(f"Telemetry delivery failed: {errors}")

    consumer = Consumer({
        "bootstrap.servers": broker,
        "group.id": metric,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([ALERT_TOPIC])
    deadline = monotonic() + 45
    try:
        while monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                # The worker may still be creating the alert topic.
                if message.error().retriable() or message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue
                raise KafkaException(message.error())
            alert = json.loads(message.value())
            if alert["metric"] == metric:
                if alert["severity"] != "critical" or alert["value"] != 980 or not alert["source"]:
                    raise RuntimeError(f"Unexpected alert: {alert}")
                print(json.dumps(alert))
                return
        raise RuntimeError(f"No alert received for {metric} within 45 seconds")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
