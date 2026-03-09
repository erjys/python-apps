import json
import os
import re
import ssl
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


def _normalize_pem(content):
    """
    Robustly normalize PEM content regardless of how Shuffle passes it.

    Shuffle collapses multiline PEM text into a single line with spaces,
    e.g: "-----BEGIN CERTIFICATE----- MIIF... -----END CERTIFICATE-----"

    This function:
      1. Finds all PEM blocks via regex (handles any whitespace)
      2. Strips ALL whitespace from base64 body
      3. Re-chunks base64 into standard 64-char lines
      4. Reconstructs valid PEM with proper newlines

    Returns normalized PEM string, or None if content is empty/invalid.
    """
    if not content or not str(content).strip():
        return None

    content = str(content).strip()

    # Match all PEM blocks regardless of whitespace formatting
    pem_blocks = re.findall(
        r'-----BEGIN ([^-]+)-----(.+?)-----END \1-----',
        content,
        re.DOTALL
    )

    if not pem_blocks:
        return None

    normalized_parts = []

    for header_type, body in pem_blocks:
        header_type = header_type.strip()

        # Remove ALL whitespace from base64 body (spaces, newlines, tabs)
        body_clean = re.sub(r'\s+', '', body.strip())

        # Re-chunk into standard 64-char lines (RFC 7468 PEM format)
        chunked = '\n'.join(
            body_clean[i:i+64] for i in range(0, len(body_clean), 64)
        )

        # Reconstruct valid PEM block
        pem_block = (
            f"-----BEGIN {header_type}-----\n"
            f"{chunked}\n"
            f"-----END {header_type}-----\n"
        )
        normalized_parts.append(pem_block)

    result = '\n'.join(normalized_parts)
    return result


class KafkaProducerWithCertificate(AppBase):
    __version__ = "1.0.0"
    app_name = "kafka-producer-with-certificate"  # must match name in api.yml

    def __init__(self, redis, logger, console_logger=None):
        super().__init__(redis, logger, console_logger)
        self.temp_cert_files = []

    def _create_temp_cert_file(self, cert_content, cert_type="certificate"):
        """
        Normalize PEM content and write to a temp file.
        Returns temp file path, or None if content is empty/invalid.
        """
        normalized = _normalize_pem(cert_content)
        if not normalized:
            self.logger.debug(f"No valid {cert_type} content, skipping")
            return None

        try:
            temp_file = tempfile.NamedTemporaryFile(
                mode='w',
                delete=False,
                suffix='.pem',
                prefix=f'kafka_{cert_type}_'
            )
            temp_file.write(normalized)
            temp_file.flush()
            temp_file.close()

            self.temp_cert_files.append(temp_file.name)
            os.chmod(temp_file.name, 0o600)

            self.logger.info(f"Created temp {cert_type} file: {temp_file.name}")
            return temp_file.name

        except Exception as e:
            raise RuntimeError(f"Failed to create temp {cert_type} file: {e}")

    def _create_cert_chain_file(self, client_cert_content, ca_cert_content):
        """
        Build a combined chain PEM file: client cert + CA cert.
        Both are normalized before combining.

        Chain order (required by TLS):
          1. Client certificate  (your identity)
          2. CA certificate      (who signed you — proves chain to broker)

        Returns temp file path, or None if no client cert provided.
        """
        client_pem = _normalize_pem(client_cert_content)
        ca_pem     = _normalize_pem(ca_cert_content)

        if not client_pem:
            self.logger.warning("No client cert content for chain — skipping")
            return None

        # Combine: client cert first, then CA
        chain_pem = client_pem
        if ca_pem:
            chain_pem = client_pem + '\n' + ca_pem
            self.logger.info("Cert chain built: [client cert] + [CA cert]")
        else:
            self.logger.warning("Cert chain built: [client cert] only — no CA appended")

        try:
            temp_file = tempfile.NamedTemporaryFile(
                mode='w',
                delete=False,
                suffix='.pem',
                prefix='kafka_chain_'
            )
            temp_file.write(chain_pem)
            temp_file.flush()
            temp_file.close()

            self.temp_cert_files.append(temp_file.name)
            os.chmod(temp_file.name, 0o600)

            self.logger.info(f"Created cert chain file: {temp_file.name}")
            return temp_file.name

        except Exception as e:
            raise RuntimeError(f"Failed to create cert chain file: {e}")

    def _cleanup_temp_files(self):
        """Remove all temporary certificate files."""
        for temp_file in self.temp_cert_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    self.logger.debug(f"Removed temp file: {temp_file}")
            except Exception as e:
                self.logger.warning(f"Could not remove temp file {temp_file}: {e}")
        self.temp_cert_files.clear()

    def _build_ssl_context(self, ca_file, chain_file, key_file, key_password):
        """
        Build a Python SSLContext configured for Kafka with self-signed CA.

        - Loads CA for broker verification (trusts self-signed CA)
        - Loads full client cert chain + private key for mutual TLS
        - Disables hostname check (self-signed certs have no matching hostname)
        """
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

        # MUST disable hostname check before setting verify_mode
        # Self-signed certs don't embed the broker hostname
        context.check_hostname = False

        # ── CA / broker verification ─────────────────────────────────────
        if ca_file:
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cafile=ca_file)
            self.logger.info(f"Broker cert verification ON — CA: {ca_file}")
        else:
            context.verify_mode = ssl.CERT_NONE
            self.logger.warning("No CA file — broker cert verification DISABLED")

        # ── Client identity (mutual TLS) ─────────────────────────────────
        if chain_file and key_file:
            try:
                password = key_password if key_password else None
                context.load_cert_chain(
                    certfile=chain_file,
                    keyfile=key_file,
                    password=password
                )
                self.logger.info("Client cert chain + key loaded into SSL context")
            except ssl.SSLError as e:
                raise RuntimeError(
                    f"Failed to load client cert/key into SSL context: {e}\n"
                    "Check: correct PEM format, key matches cert, password is correct"
                )
        else:
            self.logger.warning("No client cert/key — mutual TLS skipped")

        return context

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
        Send a message to a Kafka topic with SSL/mTLS support.
        All certificate fields accept PEM content pasted from a textbox.

        Args:
            bootstrap_servers       : Kafka broker(s) e.g. "broker:9093"
            topic                   : Target Kafka topic
            message                 : Payload — plain string or JSON
            security_protocol       : PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
            ssl_ca_content          : CA certificate PEM text (verifies broker)
            ssl_certificate_content : Client certificate PEM text (your identity)
            ssl_key_content         : Client private key PEM text
            ssl_key_password        : Key password if encrypted (optional)
            acks                    : 0 | 1 | all  (default: all)
            retries                 : Retry count on failure (default: 3)
        """

        # ── Sanitize all inputs ──────────────────────────────────────────
        bootstrap_servers = _safe_str(bootstrap_servers)
        topic             = _safe_str(topic)
        security_protocol = _safe_str(security_protocol, "PLAINTEXT").upper()
        ssl_key_password  = _safe_str(ssl_key_password)
        acks_val          = _safe_str(acks, "all")
        retries_val       = _safe_int(retries, 3)

        if acks_val not in ["0", "1", "all"]:
            self.logger.warning(f"Invalid acks '{acks_val}', defaulting to 'all'")
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
                pass  # Not JSON — send as-is

        producer = None

        try:
            # ── ssl_config dict (your exact param keys) ──────────────────
            ssl_config = {
                'bootstrap.servers':        bootstrap_servers,
                'security.protocol':        security_protocol,
                'ssl.ca.location':          None,
                'ssl.certificate.location': None,
                'ssl.key.location':         None,
                'ssl.key.password':         ssl_key_password or None,
                'acks':                     acks_val,
                'retries':                  retries_val,
            }

            # ── kafka-python producer config ─────────────────────────────
            producer_config = {
                'bootstrap_servers': ssl_config['bootstrap.servers'],
                'acks':              ssl_config['acks'],
                'retries':           ssl_config['retries'],
                'value_serializer':  lambda v: v.encode('utf-8'),
            }

            # ── SSL mode ─────────────────────────────────────────────────
            if security_protocol in ['SSL', 'SASL_SSL']:
                producer_config['security_protocol'] = security_protocol

                # 1. CA cert file — used by SSLContext to verify broker
                ssl_config['ssl.ca.location'] = self._create_temp_cert_file(
                    ssl_ca_content, "ca"
                )

                # 2. Private key file
                ssl_config['ssl.key.location'] = self._create_temp_cert_file(
                    ssl_key_content, "key"
                )

                # 3. Cert chain file = client cert + CA (full chain for broker)
                ssl_config['ssl.certificate.location'] = self._create_cert_chain_file(
                    client_cert_content=ssl_certificate_content,
                    ca_cert_content=ssl_ca_content,
                )

                # 4. Build SSLContext from validated files
                ssl_context = self._build_ssl_context(
                    ca_file      = ssl_config['ssl.ca.location'],
                    chain_file   = ssl_config['ssl.certificate.location'],
                    key_file     = ssl_config['ssl.key.location'],
                    key_password = ssl_config['ssl.key.password'],
                )
                producer_config['ssl_context'] = ssl_context
                self.logger.info("SSL context attached to producer")

            # Log sanitized config
            safe_config = {
                k: ("***" if any(s in k.lower() for s in ['password', 'key', 'secret']) else v)
                for k, v in producer_config.items()
                if k not in ('value_serializer', 'ssl_context')
            }
            self.logger.debug(f"Producer config (sanitized): {safe_config}")

            # ── Create producer ──────────────────────────────────────────
            self.logger.info("Creating Kafka producer...")
            producer = KafkaProducer(**producer_config)
            self.logger.info("Kafka producer created successfully")

            # ── Send message ─────────────────────────────────────────────
            preview = payload[:100] + "..." if len(payload) > 100 else payload
            self.logger.debug(f"Sending | topic='{topic}' | payload: {preview}")

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
