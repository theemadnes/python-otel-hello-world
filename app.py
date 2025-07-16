import asyncio
import time
import httpx
import json
import logging

# OpenTelemetry Imports
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

# --- Configure Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

# Get a tracer from the configured provider
tracer = trace.get_tracer(__name__)

@tracer.start_as_current_span("simulate_half_second_wait")
async def wait_half_second():
    """
    A simple async function that waits for half a second.
    This will be a custom span in the trace.
    """
    logger.info("Starting wait_half_second function...")
    current_span = trace.get_current_span()
    current_span.set_attribute("wait.duration.ms", 500)
    await asyncio.sleep(0.5)
    logger.info("Finished wait_half_second function.")

@tracer.start_as_current_span("fetch_jsonplaceholder_post")
async def call_demo_endpoint():
    """
    Uses httpx to call a demo JSON endpoint.
    The httpx instrumentation will automatically create a child span for the HTTP call.
    """
    url = "https://jsonplaceholder.typicode.com/posts/1"
    logger.info(f"Starting call_demo_endpoint function. Fetching from: {url}")

    current_span = trace.get_current_span()
    current_span.set_attribute("http.target.url", url)

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status() # Raise an exception for HTTP errors

            post_data = response.json()
            logger.info("Successfully fetched data from demo endpoint.")
            logger.debug("Fetched data: %s", json.dumps(post_data, indent=2))

            current_span.set_attribute("http.status_code", response.status_code)
            current_span.set_attribute("post.id", post_data.get("id"))
            current_span.set_attribute("post.title", post_data.get("title"))

    except httpx.HTTPStatusError as e:
        logger.error(
            "HTTP error occurred: %s - %s",
            e.response.status_code, e.response.text, exc_info=True
        )
        current_span.set_status(trace.Status(trace.StatusCode.ERROR,
                                              f"HTTP Error: {e.response.status_code}"))
    except httpx.RequestError as e:
        logger.error("Network error occurred: %s", e, exc_info=True)
        current_span.set_status(trace.Status(trace.StatusCode.ERROR,
                                              f"Network Error: {e}"))
    except json.JSONDecodeError as e:
        logger.error("JSON decoding error: %s", e, exc_info=True)
        current_span.set_status(trace.Status(trace.StatusCode.ERROR,
                                              f"JSON Decode Error: {e}"))
    except Exception as e:
        logger.critical("An unexpected error occurred: %s", e, exc_info=True)
        current_span.set_status(trace.Status(trace.StatusCode.ERROR,
                                              f"Unexpected Error: {e}"))

async def main():
    """
    Main function to orchestrate the operations.
    This will also be a top-level span.
    """
    logger.info("Application starting...")
    # This creates a root span for the entire application execution
    with tracer.start_as_current_span("main_application_run") as main_span:
        # Run both functions concurrently
        logger.info("Running wait_half_second and call_demo_endpoint in parallel...")
        await asyncio.gather(
            wait_half_second(),
            call_demo_endpoint()
        )
        main_span.set_attribute("app.status", "completed_parallel_tasks")
    logger.info("Application finished.")

if __name__ == "__main__":
    configure_opentelemetry()
    asyncio.run(main())
    # It's important to shut down the provider to ensure all buffered spans are sent
    trace.get_tracer_provider().shutdown()
    logger.info("OpenTelemetry provider shut down.")