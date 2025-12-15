import json
from typing import Any

import pika
from pika.exceptions import AMQPError
from walkoff_app_sdk.app_base import AppBase


class RabbitMQPublisherApp(AppBase):
    __version__ = "1.0.0"
    app_name = "rabbitmq-publisher"

    def __init__(self, redis, logger, console_logger=None):
        super().__init__(redis, logger, console_logger)

    def publish_message(
        self,
        rabbitmq_host: str,
        exchange: str,
        routing_key: str,
        message_body: str,
        username: str = "guest",
        password: str = "guest",
        port: str = "5672",
        virtual_host: str = "/",
    ) -> str:
        """Publish a message to the configured RabbitMQ exchange."""
        payload = self._prepare_message_body(message_body)

        credentials = pika.PlainCredentials(username, password)
        connection_params = pika.ConnectionParameters(
            host=rabbitmq_host,
            port=int(port),
            virtual_host=virtual_host,
            credentials=credentials,
        )

        connection = None
        channel = None
        try:
            self.logger.info(f"Connecting to RabbitMQ at {rabbitmq_host}:{port}")
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()
            channel.basic_publish(exchange=exchange, routing_key=routing_key, body=payload)
            self.logger.info(
                f"Message published to exchange '{exchange}' with routing key '{routing_key}'"
            )
            return f"Message published to exchange '{exchange}' with routing key '{routing_key}'"
        except AMQPError as exc:
            self.logger.error(f"RabbitMQ error while publishing message: {exc}")
            return f"Failed to publish message due to RabbitMQ error: {exc}"
        except Exception as exc:
            self.logger.error(f"Unexpected error while publishing message: {exc}")
            return f"Failed to publish message due to unexpected error: {exc}"
        finally:
            if channel and channel.is_open:
                try:
                    channel.close()
                except Exception as exc:
                    self.logger.warning(f"Error closing RabbitMQ channel: {exc}")
            if connection and connection.is_open:
                try:
                    connection.close()
                    self.logger.debug("RabbitMQ connection closed successfully")
                except Exception as exc:
                    self.logger.warning(f"Error closing RabbitMQ connection: {exc}")

    def _prepare_message_body(self, message_body: Any) -> bytes:
        """Ensure outgoing payload is JSON-encoded bytes."""
        payload: Any = message_body

        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                # Treat as raw string if not JSON
                return payload.encode("utf-8")

        if isinstance(payload, (dict, list)):
            return json.dumps(payload).encode("utf-8")

        return str(payload).encode("utf-8")


if __name__ == "__main__":
    RabbitMQPublisherApp.run()

