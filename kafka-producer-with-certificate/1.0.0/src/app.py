import json
import os
import re
import ssl
import tempfile
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from walkoff_app_sdk.app_base import AppBase

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def _safe_int(value, default):
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
    if value is None:
        return default
    stripped = str(value).strip()
    return stripped if stripped != "" else default


def _normalize_pem(content):
    """
    Normalize PEM content from Shuffle textboxes.
    Shuffle collapses multiline PEM into single line with spaces.
    Regex extracts all PEM blocks, strips whitespace from base64,
    and rechunks into proper 64-char RFC 7468 format.
    """
    if not content or not str(content).strip():
        return None
    content = str(content).strip()
    pem_blocks = re.findall(
        r'-----BEGIN ([^-]+)-----(.+?)-----END \1-----',
        content, re.DOTALL
    )
    if not pem_blocks:
        return None
    parts = []
    for header_type, body in pem_blocks:
        header_type = header_type.strip()
        body_clean  = re.sub(r'\s+', '', body.strip())
        chunked     = '\n'.join(body_clean[i:i+64] for i in range(0, len(body_clean), 64))
        parts.append(
            f"-----BEGIN {header_type}-----\n{chunked}\n-----END {header_type}-----\n"
        )
    return '\n'.join(parts)


def _auto_detect_protocol(security_protocol, bootstrap_servers,
                           ssl_ca_content, ssl_certificate_content, ssl_key_content):
    """
    ✅ KEY FIX: Auto-correct security_protocol when Shuffle sends wrong value.

    Rules:
    - Port 9093 + PLAINTEXT + any cert content → force SSL
    - Port 9093 + PLAINTEXT + no cert content  → force SSL (broker requires it)
    - Port 9092 + SSL                           → warn but respect user setting
    - Explicit SSL/SASL_SSL                     → always keep as-is
    """
    protocol = security_protocol.upper().strip()
    has_certs = any([
        ssl_ca_content and str(ssl_ca_content).strip(),
        ssl_certificate_content and str(ssl_certificate_content).strip(),
        ssl_key_content and str(ssl_key_content).strip(),
    ])

    # Detect port from bootstrap_servers
    port = None
    try:
        last_part = bootstrap_servers.strip().split(',')[0]  # take first broker
        port = int(last_part.split(':')[-1])
    except Exception:
        pass

    # Auto-correct: port 9093 with PLAINTEXT → SSL
    if protocol == "PLAINTEXT" and port == 9093:
        corrected = "SSL"
        return corrected, (
            f"⚠️  Auto-corrected security_protocol: PLAINTEXT → SSL "
            f"(port 9093 requires SSL). Set security_protocol=SSL in your workflow."
        )

    # Auto-correct: cert content provided but protocol is PLAINTEXT → SSL
    if protocol == "PLAINTEXT" and has_certs:
        corrected = "SSL"
        return corrected, (
            f"⚠️  Auto-corrected security_protocol: PLAINTEXT → SSL "
            f"(certificate content provided but protocol was PLAINTEXT)."
        )

    return protocol, None


class KafkaProducerWithCertificate(AppBase):
    __version__ = "1.0.0"
    app_name = "kafka-producer-with-certificate"  # must match name in api.yml

    def __init__(self, redis, logger, console_logger=None):
        super().__init__(redis, logger, console_logger)
        self.temp_cert_files = []

    def _create_temp_cert_file(self, cert_content, cert_type="certificate"):
        """Normalize and write PEM content to a temp file."""
        normalized = _normalize_pem(cert_content)
        if not normalized:
            self.logger.debug(f"No valid {cert_type} content, skipping")
            return None
        try:
            tmp = tempfile.NamedTemporaryFile(
                mode='w', delete=False,
                suffix='.pem', prefix=f'kafka_{cert_type}_'
            )
            tmp.write(normalized)
            tmp.flush()
            tmp.close()
            self.temp_cert_files.append(tmp.name)
            os.chmod(tmp.name, 0o600)
            self.logger.info(f"Created temp {cert_type} file: {tmp.name}")
            return tmp.name
        except Exception as e:
            raise RuntimeError(f"Failed to create temp {cert_type} file: {e}")

    def _create_cert_chain_file(self, client_cert_content, ca_cert_content):
        """
        Build combined chain PEM: client cert + CA cert.
        Chain order: client cert first, then CA (TLS requirement).
        """
        client_pem = _normalize_pem(client_cert_content)
        ca_pem     = _normalize_pem(ca_cert_content)

        if not client_pem:
            self.logger.warning("No client cert — skipping chain file")
            return None

        chain_pem = client_pem + ('\n' + ca_pem if ca_pem else '')
        self.logger.info(f"Cert chain: client cert + {'CA' if ca_pem else 'no CA'}")

        try:
            tmp = tempfile.NamedTemporaryFile(
                mode='w', delete=False,
                suffix='.pem', prefix='kafka_chain_'
            )
            tmp.write(chain_pem)
            tmp.flush()
            tmp.close()
            self.temp_cert_files.append(tmp.name)
            os.chmod(tmp.name, 0o600)
            self.logger.info(f"Created cert chain file: {tmp.name}")
            return tmp.name
        except Exception as e:
            raise RuntimeError(f"Failed to create cert chain file: {e}")

    def _cleanup_temp_files(self):
        for tmp in self.temp_cert_files:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
                    self.logger.debug(f"Removed temp file: {tmp}")
            except Exception as e:
                self.logger.warning(f"Could not remove {tmp}: {e}")
        self.temp_cert_files.clear()

    def _build_ssl_context(self, ca_file, chain_file, key_file, key_password):
        """Build SSLContext with self-signed CA support and mutual TLS."""
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False  # required for self-signed certs

        if ca_file:
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cafile=ca_file)
            self.logger.info(f"Broker verification ON — CA: {ca_file}")
        else:
            context.verify_mode = ssl.CERT_NONE
            self.logger.warning("No CA — broker cert verification DISABLED")

        if chain_file and key_file:
            try:
                context.load_cert_chain(
                    certfile=chain_file,
                    keyfile=key_file,
                    password=key_password if key_password else None
                )
                self.logger.info("Client cert chain + key loaded into SSL context")
            except ssl.SSLError as e:
                raise RuntimeError(f"SSL cert/key load failed: {e}")
        else:
            self.logger.warning("No client cert/key — mTLS skipped")

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

        # ── Sanitize inputs ──────────────────────────────────────────────
        bootstrap_servers = _safe_str(bootstrap_servers)
        topic             = _safe_str(topic)
        raw_protocol      = _safe_str(security_protocol, "PLAINTEXT").upper()
        ssl_key_password  = _safe_str(ssl_key_password)
        acks_val          = _safe_str(acks, "all")
        retries_val       = _safe_int(retries, 3)

        if acks_val not in ["0", "1", "all"]:
            self.logger.warning(f"Invalid acks '{acks_val}', defaulting to 'all'")
            acks_val = "all"

        # ── ✅ Auto-detect / fix security_protocol ───────────────────────
        security_protocol, correction_warning = _auto_detect_protocol(
            raw_protocol, bootstrap_servers,
            ssl_ca_content, ssl_certificate_content, ssl_key_content
        )
        if correction_warning:
            self.logger.warning(correction_warning)

        self.logger.info(
            f"Starting | brokers={bootstrap_servers} | topic={topic} "
            f"| protocol={security_protocol} (raw='{raw_protocol}') "
            f"| acks={acks_val} | retries={retries_val}"
        )

        # ── Normalize payload ────────────────────────────────────────────
        payload = message
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        elif isinstance(payload, str):
            try:
                payload = json.dumps(json.loads(payload))
            except Exception:
                pass

        producer = None

        try:
            # ── ssl_config (your exact param keys) ──────────────────────
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

            # ── Producer config ──────────────────────────────────────────
            producer_config = {
                'bootstrap_servers':           ssl_config['bootstrap.servers'],
                'acks':                        ssl_config['acks'],
                'retries':                     ssl_config['retries'],
                'value_serializer':            lambda v: v.encode('utf-8'),
                'api_version':                 (2, 6, 0),
                'request_timeout_ms':          30000,
                'api_version_auto_timeout_ms': 30000,
                'reconnect_backoff_ms':        500,
                'reconnect_backoff_max_ms':    5000,
            }

            # ── SSL mode ─────────────────────────────────────────────────
            if security_protocol in ['SSL', 'SASL_SSL']:
                producer_config['security_protocol'] = security_protocol

                ssl_config['ssl.ca.location'] = self._create_temp_cert_file(
                    ssl_ca_content, "ca"
                )
                ssl_config['ssl.key.location'] = self._create_temp_cert_file(
                    ssl_key_content, "key"
                )
                ssl_config['ssl.certificate.location'] = self._create_cert_chain_file(
                    client_cert_content=ssl_certificate_content,
                    ca_cert_content=ssl_ca_content,
                )
                ssl_context = self._build_ssl_context(
                    ca_file      = ssl_config['ssl.ca.location'],
                    chain_file   = ssl_config['ssl.certificate.location'],
                    key_file     = ssl_config['ssl.key.location'],
                    key_password = ssl_config['ssl.key.password'],
                )
                producer_config['ssl_context'] = ssl_context
                self.logger.info("SSL context attached")

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
            self.logger.info("Kafka producer created")

            # ── Send ─────────────────────────────────────────────────────
            preview = payload[:100] + "..." if len(payload) > 100 else payload
            self.logger.debug(f"Sending | topic='{topic}' | {preview}")

            future = producer.send(topic, value=payload)
            result = future.get(timeout=30)

            success_msg = (
                f"Message sent to topic '{topic}' | "
                f"partition={result.partition} | offset={result.offset}"
            )
            self.logger.info(success_msg)

            return json.dumps({
                'success':            True,
                'message':            success_msg,
                'topic':              topic,
                'partition':          result.partition,
                'offset':             result.offset,
                'protocol_used':      security_protocol,
                'protocol_corrected': raw_protocol != security_protocol,
            })

        # ── Exception handlers — ordered most-specific first ─────────────
        except NoBrokersAvailable:
            msg = (
                f"Cannot reach Kafka broker at '{bootstrap_servers}' "
                f"using protocol '{security_protocol}'. "
                "Checks: (1) Set security_protocol=SSL in workflow for port 9093, "
                "(2) Verify network/firewall from pod to broker, "
                "(3) Confirm broker hostname resolves from inside the GKE cluster"
            )
            self.logger.error(msg, exc_info=True)
            return json.dumps({
                'success':    False,
                'error':      msg,
                'error_type': 'NoBrokersAvailable',
                'hint':       f"protocol_received='{raw_protocol}', protocol_used='{security_protocol}'"
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
