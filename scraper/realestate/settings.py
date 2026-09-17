BOT_NAME = "realestate"
SPIDER_MODULES = ["realestate.spiders"]
NEWSPIDER_MODULE = "realestate.spiders"

DOWNLOAD_DELAY = 1
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 4
CONCURRENT_REQUESTS_PER_DOMAIN = 4

ROBOTSTXT_OBEY = False

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

DEFAULT_REQUEST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Referer": "https://nha.chotot.com/",
    "Origin": "https://nha.chotot.com",
}

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
}

RETRY_TIMES = 3
RETRY_HTTP_CODES = [429, 500, 502, 503, 504]

ITEM_PIPELINES = {
    "realestate.pipelines.KafkaPipeline": 300,
}

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "real_estate_raw"

LOG_LEVEL = "INFO"
FEED_EXPORT_ENCODING = "utf-8"
