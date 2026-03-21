from airflow.decorators import dag, task
from src.clients.posthog_client import PosthogClient
import pendulum
from src.services.posthog_ingest_service import load_events, load_persons


@dag(
    dag_id="posthog_data_ingestion",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 1, tz="Europe/Warsaw"),
    catchup=False,
)
def posthog_data_ingestion():

    # @task
    # def define_client():
    #     return PosthogClient()

    @task
    def ingest_events():
        return load_events()

    @task
    def ingest_persons():
        return load_persons()

    # client = define_client()
    events = ingest_events()
    persons = ingest_persons()

    events >> persons


posthog_data_ingestion()
