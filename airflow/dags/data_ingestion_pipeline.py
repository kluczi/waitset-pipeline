from airflow.decorators import dag
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # pyright: ignore[reportMissingImports]


@dag(
    dag_id="data_ingestion",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 1, tz="Europe/Warsaw"),
    catchup=False,
)
def data_ingestion():
    trigger_convex = TriggerDagRunOperator(
        task_id="trigger_convex",
        trigger_dag_id="convex_data_ingestion",
        # wait_for_completion=True,
    )

    trigger_posthog = TriggerDagRunOperator(
        task_id="trigger_posthog",
        trigger_dag_id="posthog_data_ingestion",
    )

    trigger_convex >> trigger_posthog


data_ingestion()
