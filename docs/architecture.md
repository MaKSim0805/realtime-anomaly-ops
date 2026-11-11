# Production path

`producers -> Redpanda topic -> consumer/features -> scorer -> alert topic -> Alertmanager/Grafana`

The submitted synchronous endpoint is a deliberately inspectable core. Add a Kafka consumer that calls `detect`, Prometheus counters for decisions and a rolling feature store (Redis) before production use. The detector never labels a quiet baseline as anomalous, preventing divide-by-zero false alerts.
