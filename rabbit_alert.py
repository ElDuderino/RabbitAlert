import time
import yaml
import logging
import pika
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class QueueMonitor:
    def __init__(self, name, size_threshold, inbound_rate_threshold, outbound_rate_threshold, messaging_limits,
                 channel):
        self.name = name
        self.size_threshold = size_threshold
        self.inbound_rate_threshold = inbound_rate_threshold
        self.outbound_rate_threshold = outbound_rate_threshold
        self.messaging_limits = messaging_limits
        self.last_message_times = []
        self.channel = channel
        self.previous_inbound_count = 0
        self.previous_outbound_count = 0
        self.previous_check_time = datetime.now()

    def check_thresholds(self):
        # Fetch queue metrics
        queue_state = self.channel.queue_declare(queue=self.name, passive=True)
        queue_size = queue_state.method.message_count
        current_time = datetime.now()
        elapsed_time = (current_time - self.previous_check_time).total_seconds()

        inbound_rate = (queue_size - self.previous_inbound_count) / elapsed_time
        outbound_rate = (self.previous_outbound_count - queue_size) / elapsed_time

        logging.info(
            f"Queue '{self.name}' - Size: {queue_size}, Inbound Rate: {inbound_rate}, Outbound Rate: {outbound_rate}")

        self.previous_inbound_count = queue_size
        self.previous_outbound_count = queue_size
        self.previous_check_time = current_time

        # Check thresholds
        if queue_size > self.size_threshold:
            self.send_alert(f"Queue size exceeded threshold: {queue_size}")
        if inbound_rate > self.inbound_rate_threshold:
            self.send_alert(f"Inbound message rate exceeded threshold: {inbound_rate}")
        if outbound_rate < self.outbound_rate_threshold:
            self.send_alert(f"Outbound message rate undershot threshold: {outbound_rate}")

    def send_alert(self, message):
        current_time = datetime.now()

        # Remove old message times
        self.last_message_times = [
            t for t in self.last_message_times
            if current_time - t <= timedelta(hours=self.messaging_limits['period_length_hours'])
        ]

        # Check if we can send a new message
        if len(self.last_message_times) < self.messaging_limits['max_messages_per_period']:
            if not self.last_message_times or (current_time - self.last_message_times[-1] >= timedelta(
                    minutes=self.messaging_limits['min_time_between_messages_minutes'])):
                # Stub for sending the actual message
                logging.info(f"Sending alert for {self.name}: {message}")
                self.last_message_times.append(current_time)
            else:
                logging.info(f"Alert not sent due to minimum time constraint for {self.name}")
        else:
            logging.info(f"Alert not sent due to max messages per period constraint for {self.name}")


class QueueMonitoringService:
    def __init__(self, config_file):
        self.config_file = config_file
        self.monitors = []
        self.channel = None
        self.connection = None
        self.load_configuration()

    def load_configuration(self):
        with open(self.config_file, 'r') as file:
            config = yaml.safe_load(file)

            # Load RabbitMQ connection details
            rabbitmq_config = config['rabbitmq']
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbitmq_config['host'],
                    port=rabbitmq_config['port'],
                    credentials=pika.PlainCredentials(
                        username=rabbitmq_config['username'],
                        password=rabbitmq_config['password']
                    )
                )
            )
            self.channel = self.connection.channel()

            # Load queue monitoring configurations
            for queue_config in config['queues']:
                monitor = QueueMonitor(
                    name=queue_config['name'],
                    size_threshold=queue_config['size_threshold'],
                    inbound_rate_threshold=queue_config['inbound_rate_threshold'],
                    outbound_rate_threshold=queue_config['outbound_rate_threshold'],
                    messaging_limits=queue_config['messaging_limits'],
                    channel=self.channel
                )
                self.monitors.append(monitor)
                logging.info(f"Loaded configuration for queue: {queue_config['name']}")

    def start(self):
        try:
            while True:
                for monitor in self.monitors:
                    monitor.check_thresholds()
                time.sleep(10)  # Sleep for a bit before the next check
        except KeyboardInterrupt:
            logging.info("Shutting down queue monitoring service.")
        finally:
            self.connection.close()


if __name__ == "__main__":
    service = QueueMonitoringService(config_file="config.yaml")
    service.start()
