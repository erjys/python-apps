import json
import os
import tempfile
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from walkoff_app_sdk.app_base import AppBase

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def _safe_int(value, default):
    """Safely convert value to int, returning default if empty/None/invalid."""
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip() == "":
        return default
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return default


def _safe_str(value, default=""):
    """Return stripped string or default if empty/None."""
    if value is None:
        return default
    stripped = str(value).strip()
    return stripped if stripped != "" else default


class KafkaProducerWithCertificate(AppBase):
    __version__ = "1.0.0"
    app_name = "kafka-producer-with-certificate"  # must match name in api.yml

    def __init__(self, redis, logger, console_logger=None):
        super().__init__(redis, logger, console_logger)
        self.temp_cert_files = []

    def _create_temp_cert_file(self, cert_content, cert_type="certificate"):
        """
        Write PEM certificate content string to a temporary file.
        Returns temp file path, or None if content is empty.
        """
        if not cert_content or str(cert_content).strip() == "":
            self.logger.debug(f"No {cert_type} content provided, skipping")
            return None

        try:
            temp_file = tempfile.NamedTemporaryFile(
                mode='w',
                delete=False,
                suffix='.pem',
                prefix=f'kafka_{cert_type}_'
            )
            temp_file.write(cert_content.strip())
            temp_file.flush()
            temp_file.close()

            self.temp_cert_files.append(temp_file.name)
            os.chmod(temp_file.name, 0o600)

            self.logger.info(f"Created temp {cert_type} file: {temp_file.name}")
            return temp_file.name

        except Exception as e:
            raise RuntimeError(f"Failed to create temp {cert_type} file: {e}")

    def _cleanup_temp_files(self):
        """Remove all temporary certificate files created during execution."""
        for temp_file in self.temp_cert_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    self.logger.debug(f"Removed temp file: {temp_file}")
            except Exception as e:
                self.logger.warning(f"Could not remove temp file {temp_file}: {e}")
        self.temp_cert_files.clear()

    def send_message_to_kafka_topic(
        self,
        bootstrap_servers: str,
        topic: str,
        message: str,
        security_protocol: str = "PLAINTEXT",
        ssl_ca_content: str = "",
        ssl_certificate_content: str = "",
        ssl_key_content: str = "",
        ssl_key_password: str = "",
        acks: str = "all",
        retries: str = "3",
    ) -> str:
        """
        Send a message to a Kafka topic with configurable SSL security settings.

        Args:
            bootstrap_servers : Kafka broker address(es), e.g. "broker:9093"
            topic             : Target Kafka topic name
            message           : Message payload (plain string or JSON)
            security_protocol : PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
            ssl_ca_content    : CA certificate PEM content (paste from textbox)
            ssl_certificate_content : Client certificate PEM content
            ssl_key_content   : Client private key PEM content
            ssl_key_password  : Password for encrypted private key (optional)
            acks              : Producer ack level — 0 | 1 | all  (default: all)
            retries           : Number of retries on failure (default: 3)

        Returns:
            JSON string with success status and delivery details
        """

        # ── Sanitize inputs ──────────────────────────────────────────────
        bootstrap_servers = _safe_str(bootstrap_servers)
        topic             = _safe_str(topic)
        security_protocol = _safe_str(security_protocol, "PLAINTEXT").upper()
        ssl_key_password  = _safe_str(ssl_key_password)
        acks_val          = _safe_str(acks, "all")
        retries_val       = _safe_int(retries, 3)

        # Validate acks
        if acks_val not in ["0", "1", "all"]:
            self.logger.warning(f"Invalid acks value '{acks_val}', defaulting to 'all'")
            acks_val = "all"

        self.logger.info(
            f"Starting | brokers={bootstrap_servers} | topic={topic} "
            f"| protocol={security_protocol} | acks={acks_val} | retries={retries_val}"
        )

        # ── Normalize message payload ────────────────────────────────────
        payload = message
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        elif isinstance(payload, str):
            try:
                payload = json.dumps(json.loads(payload))
            except Exception:
                pass  # Not JSON — send as plain string

        producer = None

        try:
            # ── Build ssl_config matching your exact param keys ──────────
            ssl_config = {
                'bootstrap.servers':        bootstrap_servers,
                'security.protocol':        security_protocol,
                'ssl.ca.location':          None,
                'ssl.certificate.location': None,
                'ssl.key.location':         None,
                'ssl.key.password':         ssl_key_password if ssl_key_password else None,
                'acks':                     acks_val,
                'retries':                  retries_val,
            }

            # ── Create temp cert files from pasted content ───────────────
            if security_protocol in ['SSL', 'SASL_SSL']:
                self.logger.info(f"SSL/TLS enabled: {security_protocol}")

                ssl_config['ssl.ca.location'] = self._create_temp_cert_file(
                    ssl_ca_content, "ca"
                )
                ssl_config['ssl.certificate.location'] = self._create_temp_cert_file(
                    ssl_certificate_content, "cert"
                )
                ssl_config['ssl.key.location'] = self._create_temp_cert_file(
                    ssl_key_content, "key"
                )

            # ── Map ssl_config keys → kafka-python producer config ───────
            producer_config = {
                'bootstrap_servers':  ssl_config['bootstrap.servers'],
                'security_protocol':  ssl_config['security.protocol'],
                'acks':               ssl_config['acks'],
                'retries':            ssl_config['retries'],
                'value_serializer':   lambda v: v.encode('utf-8'),
            }

            # Only add SSL file paths if they were actually created
            if ssl_config['ssl.ca.location']:
                producer_config['ssl_cafile'] = ssl_config['ssl.ca.location']

            if ssl_config['ssl.certificate.location']:
                producer_config['ssl_certfile'] = ssl_config['ssl.certificate.location']

            if ssl_config['ssl.key.location']:
                producer_config['ssl_keyfile'] = ssl_config['ssl.key.location']

            if ssl_config['ssl.key.password']:
                producer_config['ssl_password'] = ssl_config['ssl.key.password']

            # Remove security_protocol for PLAINTEXT (not needed)
            if security_protocol == "PLAINTEXT":
                producer_config.pop('security_protocol', None)

            # Log sanitized config
            safe_config = {
                k: ("***" if any(s in k.lower() for s in ['password', 'key', 'secret']) else v)
                for k, v in producer_config.items()
                if k != 'value_serializer'
            }
            self.logger.debug(f"Producer config (sanitized): {safe_config}")

            # ── Create Kafka producer ────────────────────────────────────
            self.logger.info("Creating Kafka producer...")
            producer = KafkaProducer(**producer_config)
            self.logger.info("Kafka producer created successfully")

            # ── Send message ─────────────────────────────────────────────
            self.logger.debug(f"Sending to topic='{topic}' | payload preview: {payload[:100]}")
            future = producer.send(topic, value=payload)
            result = future.get(timeout=30)

            success_msg = (
                f"Message sent to topic '{topic}' | "
                f"partition={result.partition} | offset={result.offset}"
            )
            self.logger.info(success_msg)

            return json.dumps({
                'success':   True,
                'message':   success_msg,
                'topic':     topic,
                'partition': result.partition,
                'offset':    result.offset,
            })

        except RuntimeError as e:
            msg = f"Certificate setup error: {e}"
            self.logger.error(msg)
            return json.dumps({'success': False, 'error': msg, 'error_type': 'CertificateError'})

        except KafkaError as e:
            msg = f"Kafka error: {e}"
            self.logger.error(msg, exc_info=True)
            return json.dumps({'success': False, 'error': msg, 'error_type': 'KafkaError'})

        except Exception as e:
            msg = f"Unexpected error: {e}"
            self.logger.error(msg, exc_info=True)
            return json.dumps({'success': False, 'error': msg, 'error_type': type(e).__name__})

        finally:
            if producer:
                try:
                    producer.flush(timeout=5)
                    producer.close(timeout=5)
                    self.logger.info("Kafka producer closed")
                except Exception as e:
                    self.logger.warning(f"Error closing producer: {e}")

            self._cleanup_temp_files()


if __name__ == "__main__":
    KafkaProducerWithCertificate.run()
