import os
from opentelemetry import metrics
from opentelemetry import trace

TRACER = tracer = trace.get_tracer(
    os.getenv("OTEL_SERVICE_NAME", "no-service-name"), os.getenv("VERSION", "0.0.0")
)

METER = metrics.get_meter("global.meter")
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "")

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BATCH_TIME = int(os.getenv("BATCH_TIME", "5"))

MYSQL_CRYPTO_USER = (os.getenv("MYSQL_CRYPTO_USER"),)
MYSQL_CRYPTO_PASSWORD = (os.getenv("MYSQL_CRYPTO_PASSWORD"),)
MYSQL_CRYPTO_SERVER = (os.getenv("MYSQL_CRYPTO_SERVER"),)
MYSQL_CRYPTO_DB = (os.getenv("MYSQL_CRYPTO_DB"),)
