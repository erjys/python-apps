import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from walkoff_app_sdk.app_base import AppBase


class KafkaProducerApp(AppBase):
    __version__ = "1.0.0"
    app_name = "kafka-producer"  # must match name in api.yml

    def __init__(self, redis, logger, console_logger=None):
        super().__init__(redis, logger, console_logger)

    def send_message_to_kafka_topic(self, bootstrap_servers: str, topic: str, message: str, json_data: str = "") -> str:
        import json

        # Always use 'message' parameter; ignore 'json_data'
        payload = message

        # If message is dict or list (JSON-like Python structure), serialize as JSON string
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        elif isinstance(payload, str):
            # Try to load as JSON; if yes, re-serialize for stable output, if not, leave as-is
            try:
                loaded_json = json.loads(payload)
                payload = json.dumps(loaded_json)
            except Exception:
                pass  # Not JSON, just send the string as-is

        producer = None
        try:
            self.logger.info(f"Connecting to Kafka brokers at {bootstrap_servers}")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: v.encode("utf-8"),
                request_timeout_ms=10000,
                retries=3,
            )
            self.logger.debug(f"Sending message to topic '{topic}': {payload}")

            future = producer.send(topic, value=payload)
            result = future.get(timeout=10)

            self.logger.info(f"Message sent to topic {topic} at offset {result.offset}")
            return f"Message sent to topic {topic} at offset {result.offset}"
        except KafkaError as e:
            self.logger.error(f"Kafka error while sending message: {e}")
            return f"Failed to send message due to Kafka error: {e}"
        except Exception as e:
            self.logger.error(f"Unexpected error while sending message: {e}")
            return f"Failed to send message due to unexpected error: {e}"
        finally:
            if producer:
                try:
                    producer.flush()
                    producer.close()
                    self.logger.debug("Kafka producer closed successfully")
                except Exception as e:
                    self.logger.warning(f"Error closing Kafka producer: {e}")

if __name__ == "__main__":
    KafkaProducerApp.run()
