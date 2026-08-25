import logging

LOG_PATH = "clickstream.log"
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)

logger = logging.getLogger("clickstream_pipeline")