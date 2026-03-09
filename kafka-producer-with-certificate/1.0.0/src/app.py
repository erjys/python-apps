import json
import os
import tempfile
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from walkoff_app_sdk.app_base import AppBase

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class KafkaProducerWithCertificate(AppBase):
    __version__ = "1.0.0"
    app_name = "kafka-producer-with-certificate"  # must match name in api.yml

    def __init__(self, redis, logger, console_logger=None):
        super().__init__(redis, logger, console_logger)
        self.temp_cert_files = []  # Track temporary files for cleanup

    def _create_temp_cert_file(self, cert_content, cert_type="certificate"):
        """
        Create a temporary file from certificate content string.
        
        Args:
            cert_content: Certificate content as string
            cert_type: Type of certificate for logging
            
        Returns:
            Path to temporary file or None if content is empty
        """
        if not cert_content or cert_content.strip() == "":
            self.logger.debug(f"No {cert_type} content provided, skipping file creation")
            return None
        
        try:
            # Create temporary file that persists until explicitly deleted
            temp_file = tempfile.NamedTemporaryFile(
                mode='w',
                delete=False,
                suffix='.pem',
                prefix=f'kafka_{cert_type}_'
            )
            
            # Write certificate content
            temp_file.write(cert_content.strip())
            temp_file.flush()
            temp_file.close()
            
            # Track for cleanup
            self.temp_cert_files.append(temp_file.name)
            
            # Set appropriate permissions (readable by owner)
            os.chmod(temp_file.name, 0o600)
            
            self.logger.info(f"Created temporary {cert_type} file: {temp_file.name}")
            self.logger.debug(f"{cert_type} content length: {len(cert_content)} characters")
            
            return temp_file.name
            
        except Exception as e:
            error_msg = f"Failed to create temporary {cert_type} file: {str(e)}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _cleanup_temp_files(self):
        """Clean up all temporary certificate files."""
        for temp_file in self.temp_cert_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                    self.logger.debug(f"Cleaned up temporary file: {temp_file}")
            except Exception as e:
                self.logger.warning(f"Failed to cleanup temp file {temp_file}: {e}")
        
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
        sasl_mechanism: str = "",
        sasl_username: str = "",
        sasl_password: str = "",
        acks: str = "all",
        retries: str = "3",
        timeout_ms: str = "10000",
        message_key: str = "",
        json_data: str = ""  # Keep for backward compatibility but ignore
    ) -> str:
        """
        Send a message to a Kafka topic with configurable security settings.
        Certificate content is provided as text instead of file paths.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses (e.g., "localhost:9092")
            topic: Kafka topic name
            message: Message payload to send
            security_protocol: Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
            ssl_ca_content: CA certificate content (PEM format) as text
            ssl_certificate_content: Client certificate content (PEM format) as text
            ssl_key_content: Client private key content (PEM format) as text
            ssl_key_password: Password for encrypted client key (optional)
            sasl_mechanism: SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI)
            sasl_username: SASL username
            sasl_password: SASL password
            acks: Acknowledgment level (0, 1, all)
            retries: Number of retries (default: 3)
            timeout_ms: Request timeout in milliseconds (default: 10000)
            message_key: Optional message key for partitioning
            
        Returns:
            JSON string with success status and details
        """
        # Process message payload
        payload = message
        
        # Handle JSON serialization
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload)
        elif isinstance(payload, str):
            try:
                loaded_json = json.loads(payload)
                payload = json.dumps(loaded_json)
            except Exception:
                pass  # Not JSON, send as-is

        producer = None
        
        try:
            # Create temporary files from certificate content
            cert_files = {
                'ca': None,
                'cert': None,
                'key': None
            }
            
            if security_protocol in ['SSL', 'SASL_SSL']:
                self.logger.info(f"SSL/TLS enabled with protocol: {security_protocol}")
                
                # Create temp files from certificate content
                if ssl_ca_content:
                    cert_files['ca'] = self._create_temp_cert_file(
                        ssl_ca_content, "ca_certificate"
                    )
                    self.logger.info("CA certificate loaded from content")
                
                if ssl_certificate_content:
                    cert_files['cert'] = self._create_temp_cert_file(
                        ssl_certificate_content, "client_certificate"
                    )
                    self.logger.info("Client certificate loaded from content")
                
                if ssl_key_content:
                    cert_files['key'] = self._create_temp_cert_file(
                        ssl_key_content, "client_key"
                    )
                    self.logger.info("Client key loaded from content")

            # Build producer configuration
            self.logger.info(f"Connecting to Kafka brokers at {bootstrap_servers}")
            
            producer_config = {
                'bootstrap_servers': bootstrap_servers,
                'value_serializer': lambda v: v.encode('utf-8'),
                'request_timeout_ms': int(timeout_ms),
                'retries': int(retries),
                'acks': acks if acks in ['0', '1', 'all'] else 'all',
            }

            # Add SSL configuration
            if security_protocol in ['SSL', 'SASL_SSL']:
                producer_config['security_protocol'] = security_protocol
                
                if cert_files['ca']:
                    producer_config['ssl_cafile'] = cert_files['ca']
                    self.logger.debug(f"Using CA cert file: {cert_files['ca']}")
                
                if cert_files['cert']:
                    producer_config['ssl_certfile'] = cert_files['cert']
                    self.logger.debug(f"Using client cert file: {cert_files['cert']}")
                
                if cert_files['key']:
                    producer_config['ssl_keyfile'] = cert_files['key']
                    self.logger.debug(f"Using client key file: {cert_files['key']}")
                
                if ssl_key_password and ssl_key_password.strip():
                    producer_config['ssl_password'] = ssl_key_password
                    self.logger.debug("SSL key password provided")
                
                # Optional SSL settings (uncomment if needed)
                # producer_config['ssl_check_hostname'] = False
                # producer_config['ssl_crlfile'] = None
                
                self.logger.debug("SSL configuration completed")

            # Add SASL configuration
            if security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
                producer_config['security_protocol'] = security_protocol
                
                if sasl_mechanism and sasl_mechanism.strip():
                    producer_config['sasl_mechanism'] = sasl_mechanism.upper()
                    self.logger.debug(f"SASL mechanism: {sasl_mechanism}")
                else:
                    # Default to PLAIN if not specified
                    producer_config['sasl_mechanism'] = 'PLAIN'
                    self.logger.debug("SASL mechanism defaulted to PLAIN")
                
                if sasl_username and sasl_username.strip():
                    producer_config['sasl_plain_username'] = sasl_username
                    self.logger.debug(f"SASL username: {sasl_username}")
                
                if sasl_password and sasl_password.strip():
                    producer_config['sasl_plain_password'] = sasl_password
                    self.logger.debug("SASL password provided")
                
                self.logger.debug("SASL configuration completed")

            # Log final configuration (without sensitive data)
            safe_config = {k: v for k, v in producer_config.items() 
                          if 'password' not in k.lower()}
            self.logger.debug(f"Producer config (sanitized): {safe_config}")

            # Create Kafka producer
            self.logger.info("Creating Kafka producer...")
            producer = KafkaProducer(**producer_config)
            self.logger.info("Kafka producer created successfully")
            
            # Prepare message
            self.logger.debug(f"Preparing to send message to topic '{topic}'")
            if len(payload) > 100:
                self.logger.debug(f"Message preview: {payload[:100]}... (truncated)")
            else:
                self.logger.debug(f"Message content: {payload}")
            
            # Send message with optional key
            if message_key and message_key.strip():
                key_bytes = message_key.encode('utf-8')
                self.logger.debug(f"Sending with message key: {message_key}")
                future = producer.send(topic, key=key_bytes, value=payload)
            else:
                self.logger.debug("Sending without message key")
                future = producer.send(topic, value=payload)
            
            # Wait for message to be delivered
            self.logger.debug("Waiting for message delivery confirmation...")
            result = future.get(timeout=int(timeout_ms) / 1000)
            
            success_msg = (
                f"Message sent successfully to topic '{topic}' "
                f"at partition {result.partition}, offset {result.offset}"
            )
            self.logger.info(success_msg)
            
            response = {
                'success': True,
                'message': success_msg,
                'topic': topic,
                'partition': result.partition,
                'offset': result.offset,
                'timestamp': result.timestamp
            }
            
            return json.dumps(response)

        except RuntimeError as e:
            # Certificate file creation errors
            error_msg = f"Certificate setup error: {str(e)}"
            self.logger.error(error_msg)
            return json.dumps({'success': False, 'error': error_msg})
        
        except KafkaError as e:
            error_msg = f"Kafka error while sending message: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return json.dumps({'success': False, 'error': error_msg, 'error_type': 'KafkaError'})
        
        except ValueError as e:
            error_msg = f"Configuration error: {str(e)}"
            self.logger.error(error_msg)
            return json.dumps({'success': False, 'error': error_msg, 'error_type': 'ValueError'})
        
        except TimeoutError as e:
            error_msg = f"Timeout error: {str(e)}"
            self.logger.error(error_msg)
            return json.dumps({'success': False, 'error': error_msg, 'error_type': 'TimeoutError'})
        
        except Exception as e:
            error_msg = f"Unexpected error while sending message: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return json.dumps({'success': False, 'error': error_msg, 'error_type': type(e).__name__})
        
        finally:
            # Close producer
            if producer:
                try:
                    self.logger.debug("Flushing producer...")
                    producer.flush(timeout=5)
                    self.logger.debug("Closing producer...")
                    producer.close(timeout=5)
                    self.logger.info("Kafka producer closed successfully")
                except Exception as e:
                    self.logger.warning(f"Error closing Kafka producer: {e}")
            
            # Cleanup temporary certificate files
            self._cleanup_temp_files()
            self.logger.debug("Temporary certificate files cleaned up")


if __name__ == "__main__":
    KafkaProducerWithCertificate.run()
