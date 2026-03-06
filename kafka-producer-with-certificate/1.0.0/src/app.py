import json
import sys
import logging
import os
import traceback
from confluent_kafka import Producer
from walkoff_app_sdk.app_base import Shuffle
 
# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _validate_certificate_file(file_path, file_type="certificate"):
    """
    Validate that certificate file exists and is readable.
    
    Args:
        file_path: Path to certificate file
        file_type: Type of certificate (for logging)
    
    Returns:
        Validated file path
    
    Raises:
        FileNotFoundError: If file doesn't exist
        PermissionError: If file is not readable
    """
    if not file_path:
        return None
    
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"{file_type} file not found: {file_path}")
    
    if not os.access(file_path, os.R_OK):
        raise PermissionError(f"{file_type} file is not readable: {file_path}")
    
    logger.info(f"Validated {file_type} file: {file_path}")
    return file_path

def send_message_to_kafka_topic(
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
    """
    Send a message to a Kafka topic with configurable security and producer settings.
    
    Supports certificate uploads for SSL/TLS and secure authentication:
    - Upload .pem files directly through Shuffle UI
    - Files are handled securely by Shuffle SDK
    - No need for base64 encoding or secrets management
    
    Args:
        bootstrap_servers: Comma-separated broker addresses (e.g., "kafka:9092")
        topic: Target Kafka topic name
        message: Message to publish (string or JSON)
        security_protocol: Protocol type (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
        ssl_ca_location: Uploaded CA certificate file (.pem)
        ssl_certificate_location: Uploaded client certificate file (.pem)
        ssl_key_location: Uploaded client key file (.pem)
        ssl_key_password: Password for client key
        sasl_mechanism: SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
        sasl_username: SASL username
        sasl_password: SASL password
        acks: Acknowledgement level (0, 1, all)
        retries: Number of retry attempts
        timeout_ms: Request timeout in milliseconds
        message_key: Optional message key for partitioning
    
    Returns:
        Dictionary with success status and delivery information
    """
    
    try:
        # Validate and prepare certificate files if provided
        cert_files = {
            'ca': None,
            'cert': None,
            'key': None
        }
        
        if security_protocol in ['SSL', 'SASL_SSL']:
            try:
                if ssl_ca_location:
                    cert_files['ca'] = _validate_certificate_file(ssl_ca_location, "CA certificate")
                if ssl_certificate_location:
                    cert_files['cert'] = _validate_certificate_file(ssl_certificate_location, "Client certificate")
                if ssl_key_location:
                    cert_files['key'] = _validate_certificate_file(ssl_key_location, "Client key")
            except (FileNotFoundError, PermissionError) as e:
                logger.error(f"Certificate validation failed: {str(e)}")
                return {
                    'success': False,
                    'message': f'Certificate validation failed: {str(e)}',
                    'topic': topic,
                    'partition': None,
                    'offset': None,
                    'error': str(e)
                }
        
        # Build producer configuration
        config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': acks,
            'retries': int(retries),
            'socket.timeout.ms': int(timeout_ms),
            'security.protocol': security_protocol,
        }
        
        # Add SSL configuration with certificate file paths
        if security_protocol in ['SSL', 'SASL_SSL']:
            if cert_files['ca']:
                config['ssl.ca.location'] = cert_files['ca']
            if cert_files['cert']:
                config['ssl.certificate.location'] = cert_files['cert']
            if cert_files['key']:
                config['ssl.key.location'] = cert_files['key']
            if ssl_key_password:
                config['ssl.key.password'] = ssl_key_password
        
        # Add SASL configuration if using SASL protocols
        if security_protocol in ['SASL_PLAINTEXT', 'SASL_SSL']:
            if sasl_mechanism:
                config['sasl.mechanism'] = sasl_mechanism
            if sasl_username:
                config['sasl.username'] = sasl_username
            if sasl_password:
                config['sasl.password'] = sasl_password
        
        logger.info(f"Creating Kafka producer with config: {config}")
        
        # Storage for delivery callback results
        delivery_info = {
            'success': False,
            'partition': None,
            'offset': None,
            'error': None
        }
        
        # Callback function for delivery reports
        def delivery_report(err, msg):
            if err is not None:
                logger.error(f'Message delivery failed: {err}')
                delivery_info['success'] = False
                delivery_info['error'] = str(err)
            else:
                logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
                delivery_info['success'] = True
                delivery_info['partition'] = msg.partition()
                delivery_info['offset'] = msg.offset()
        
        # Create Kafka producer
        producer = Producer(config)
        
        # Prepare the message
        if isinstance(message, str):
            # Try to parse as JSON, if it fails, treat as string
            try:
                parsed_message = json.loads(message)
                message_bytes = json.dumps(parsed_message).encode('utf-8')
            except (json.JSONDecodeError, ValueError):
                message_bytes = message.encode('utf-8')
        else:
            message_bytes = json.dumps(message).encode('utf-8')
        
        # Prepare the key
        key_bytes = None
        if message_key:
            key_bytes = message_key.encode('utf-8')
        
        # Produce the message
        logger.info(f"Publishing message to topic '{topic}'")
        producer.produce(
            topic,
            value=message_bytes,
            key=key_bytes,
            callback=delivery_report
        )
        
        # Flush to ensure delivery
        producer.flush()
        
        return {
            'success': delivery_info['success'],
            'message': 'Message published successfully' if delivery_info['success'] else 'Message delivery failed',
            'topic': topic,
            'partition': delivery_info['partition'],
            'offset': delivery_info['offset'],
            'error': delivery_info['error']
        }
    
    except Exception as e:
        logger.error(f"Error producing message: {str(e)}", exc_info=True)
        return {
            'success': False,
            'message': f'Error: {str(e)}',
            'topic': topic,
            'partition': None,
            'offset': None,
            'error': str(e)
        }


if __name__ == "__main__":
    try:
        # Initialize Shuffle SDK for workflow integration
        client = Shuffle()
        logger.info("Starting Shuffle app - kafka-producer-with-certificate")
        
        # Get action
        client.run_app()
    except Exception as e:
        logger.error(f"Fatal error in app execution: {str(e)}", exc_info=True)
        sys.exit(1)
