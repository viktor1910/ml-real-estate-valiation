import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaPipeline:
    def open_spider(self, spider):
        self.producer = KafkaProducer(
            bootstrap_servers=spider.settings.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            retries=3,
        )
        self.topic = spider.settings.get("KAFKA_TOPIC", "real_estate_raw")
        logger.info(f"Kafka producer connected, topic={self.topic}")

    def close_spider(self, spider):
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")

    def process_item(self, item, spider):
        try:
            self.producer.send(self.topic, dict(item))
        except KafkaError as e:
            logger.error(f"Failed to send item to Kafka: {e}")
        return item
