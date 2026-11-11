# Production path

`producers -> Redpanda topic -> consumer/features -> scorer -> alert topic -> Alertmanager/Grafana`

The worker and HTTP endpoint share the same detector. The worker builds its baseline from events in Kafka, while the synchronous endpoint accepts a caller-provided baseline. A zero-variance window returns a zero z-score under the current scoring policy. Durable feature state and downstream alert delivery are future extensions.
