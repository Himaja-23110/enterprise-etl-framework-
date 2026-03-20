import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def on_failure_callback(context):
    """Called when a task fails."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')

    logger.error(
        f"ALERT: Task Failed!\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution Date: {execution_date}\n"
        f"Error: {exception}"
    )
    print(f"🚨 FAILURE ALERT: {dag_id}.{task_id} failed at {execution_date}")


def on_success_callback(context):
    """Called when a task succeeds."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']

    logger.info(
        f"SUCCESS: Task Completed!\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution Date: {execution_date}"
    )
    print(f"✅ SUCCESS ALERT: {dag_id}.{task_id} completed at {execution_date}")


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when SLA is missed."""
    logger.warning(
        f"SLA MISS ALERT!\n"
        f"DAG: {dag.dag_id}\n"
        f"Missed tasks: {[t.task_id for t in task_list]}"
    )
    print(f"⚠️ SLA MISS: {dag.dag_id} missed SLA!")