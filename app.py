import httpx
import asyncio
import json
import time
import logging

# OpenTelemetry Imports
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

WAIT_TIME=0.5

logging.basicConfig(
    level=logging.INFO,  # Set the minimum level of messages to display (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Get a logger instance for this module (good practice)
logger = logging.getLogger(__name__)

# --- Configure OpenTelemetry ---
def configure_opentelemetry():
    """
    Configures OpenTelemetry to send traces to Google Cloud Trace.
    """
    # Define your service resource
    resource = Resource.create({
        "service.name": "my-python-app",
        "service.version": "1.0.0",
        "environment": "development",
        # You can add more attributes here, e.g., "host.name": "my-machine"
    })

    # Set up the TracerProvider with the defined resource
    provider = TracerProvider(resource=resource)

    # Configure Cloud Trace Exporter
    # BatchSpanProcessor is recommended for production to buffer and send spans efficiently
    cloud_trace_exporter = CloudTraceSpanExporter()
    span_processor = BatchSpanProcessor(cloud_trace_exporter)
    provider.add_span_processor(span_processor)

    # Set the global tracer provider
    trace.set_tracer_provider(provider)

    # Instrument httpx automatically
    # This will create spans for HTTP requests made with httpx
    HTTPXClientInstrumentor().instrument()

    logger.info("OpenTelemetry configured successfully for Google Cloud Trace.")

def just_waste_time():
    time.sleep(WAIT_TIME)
    logger.info(f"Slept for {WAIT_TIME} seconds")

async def fetch_jsonplaceholder_post():
    """
    Fetches a single post from JSONPlaceholder using httpx in an async context.
    """
    url = "https://jsonplaceholder.typicode.com/posts/1"

    logger.info(f"Making GET request to: {url}")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)

            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()

            # The response.json() method automatically parses the JSON
            post_data = response.json()

            logger.info("--- Response Data ---")
            logger.info(json.dumps(post_data, indent=2))
            logger.info(f"Status Code: {response.status_code}")
            logger.info(f"Content-Type: {response.headers.get('content-type')}")

    except httpx.HTTPStatusError as e:
        logger.info(f"HTTP error occurred: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        logger.info(f"An error occurred while requesting {e.request.url!r}: {e}")
    except Exception as e:
        logger.info(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    just_waste_time()
    asyncio.run(fetch_jsonplaceholder_post())