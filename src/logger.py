import logging

logger = logging.getLogger("btc-real-time-data-streaming")
logger.setLevel(logging.INFO)  # Set the logging level

# Add a console handler (prints logs to stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Ensure INFO messages are logged
logger.addHandler(console_handler)
