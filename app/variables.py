import os
from opentelemetry import metrics
from opentelemetry import trace
from dotenv import load_dotenv

load_dotenv()

TRACER = tracer = trace.get_tracer(
    os.getenv("OTEL_SERVICE_NAME", "no-service-name"), os.getenv("VERSION", "0.0.0")
)

METER = metrics.get_meter("global.meter")
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "default-service-name")
SVC = OTEL_SERVICE_NAME

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))
SQL_HANDLER_BATCH_SIZE = int(os.getenv("SQL_HANDLER_BATCH_SIZE", "500"))
SQL_HANDLER_BATCH_TIME = int(os.getenv("SQL_HANDLER_BATCH_TIME", "120"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

os.environ["MYSQL_USER"] = os.getenv("MYSQL_CRYPTO_USER", "")
os.environ["MYSQL_PASSWORD"] = os.getenv("MYSQL_CRYPTO_PASSWORD", "")
os.environ["MYSQL_SERVER"] = os.getenv("MYSQL_CRYPTO_SERVER", "")
os.environ["MYSQL_DB"] = os.getenv("MYSQL_CRYPTO_DB", "")
