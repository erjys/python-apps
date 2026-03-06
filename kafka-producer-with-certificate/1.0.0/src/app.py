import json
import logging
import os
from confluent_kafka import Producer
from walkoff_app_sdk.app_base import AppBase

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class KafkaProducerWithCertificate(AppBase):
    __version__ = "1.0.0"
    app_name = "kafka-producer-with-certificate"  # must match name in api.yml

    def __init__(self, redis, logger, console_logger=None):
        super().__init__(redis, logger, console_logger)

    def _validate_certificate_file(self, file_path, file_type="certificate"):
        if not file_path:
            return None
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"{file_type} file not found: {file_path}")
        if not os.access(file_path, os.R_OK):
            raise PermissionError(f"{file_type} file is not readable: {file_path}")
        self.logger.info(f"Validated {file_type} file: {file_path}")
        return file_path

    def send_message_to_kafka_topic(
        self,
        bootstrap_servers,
        topic,
        message,
        security_protocol="PLAINTEXT",
        ssl_ca_location=None,
        ssl_certificate_location=None,
        ssl_key_location=None,
        ssl_key_password=None,
        sasl_mechanism=None,
        sasl_username=None,
        sasl_password=None,
        acks="all",
        retries="5",
        timeout_ms="30000",
        message_key=None
    ):
        """Send a message to a Kafka topic with configurable security settings."""
        try:
            # Validate certificate files
            cert_files = {'ca': None, 'cert': None, 'key': None}
            
            if security_protocol in ['SSL', 'SASL_SSL']:
                if ssl_ca_location:
                    cert_files['ca'] = self._validate_certificate_file(ssl_ca_location, "CA certificate")
                if ssl_certificate_location:
                    cert_files['cert'] = self._validate_certificate_file(ssl_certificate_location, "Client certificate")
                if ssl_key_location:
                    cert_files['key'] = self._validate_certificate_file(ssl_key_location, "Client key")

            # Build producer configuration
            config = {
                'bootstrap.servers': bootstrap_servers,
                'acks': acks,
                'retries': int(retries),
                'socket.timeout.ms': int(timeout_ms),
                'security.protocol': security_protocol,
            }

            # Add SSL configuration
            if security_protocol in ['SSL', 'SASL_SSL']:
                if cert_files['ca']:
                    config['ssl.ca.location'] = cert_files['ca']
                if cert_files['cert']:
                    config['ssl.certificate.location'] = cert_files['cert']
                if cert_files['key']:
                    config['ssl.key.location'] = cert_files['key']
                if ssl_key_password:
                    config['ssl.key.password'] = ssl_key_password

            # Add SASL configuration
            if security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
                if sasl_mechanism:
                    config['sasl.mechanism'] = sasl_mechanism
                if sasl_username:
                    config['sasl.username'] = sasl_username
                if sasl_password:
                    config['sasl.password'] = sasl_password

            # Create producer and send message
            producer = Producer(config)
            
            if message_key:
                producer.produce(topic, key=message_key.encode('utf-8'), value=message.encode('utf-8'))
            else:
                producer.produce(topic, value=message.encode('utf-8'))
            
            producer.flush()
            self.logger.info(f"Message sent successfully to topic: {topic}")
            return json.dumps({'success': True, 'message': 'Message sent successfully'})

        except Exception as e:
            self.logger.error(f"Error sending message to Kafka: {str(e)}")
            return json.dumps({'success': False, 'error': str(e)})
