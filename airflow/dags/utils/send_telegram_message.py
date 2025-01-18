from airflow.providers.telegram.operators.telegram import TelegramOperator
import os


def send_telegram_message(message, context, task_id):
    """
    Reusable function to send a message via Telegram.
    """

    chat_id = os.getenv("TELEGRAM_BOT_CHAT_ID")

    telegram_task = TelegramOperator(
        task_id=task_id,
        telegram_kwargs={"parse_mode": "markdown"},
        text=message,
        chat_id=chat_id,
    )
    telegram_task.execute(context)


def send_telegram_failure_message(context):
    """
    Sends a failure notification via Telegram.
    """

    title = f"Task *{context['task_instance'].task_id}* Failed"
    dag_id = context["dag"].dag_id if context.get("dag") else "Unknown DAG"
    task_id = (
        context["task_instance"].task_id
        if context.get("task_instance")
        else "Unknown Task"
    )
    execution_date = context.get("execution_date", "Unknown Execution Date")
    log_url = (
        context["task_instance"].log_url
        if context.get("task_instance")
        else "No Log URL Available"
    )
    exception = context.get("exception", "No exception provided")

    failure_message = (
        f"*🗂️ DAG ID*: `{dag_id}`\n"
        f"*📌 Task ID*: `{task_id}`\n"
        f"*🚨 Status*: `Failed`\n"
        f"*📅 Execution Date*: `{execution_date}`\n"
        f"📄 *Log URL*: [View Logs]({log_url})\n\n"
    )

    send_telegram_message(
        message=failure_message, context=context, task_id="send_failure_message_task"
    )


def send_telegram_retry_message(context):
    """
    Sends a retry notification via Telegram.
    """

    dag_id = context["dag"].dag_id if context.get("dag") else "Unknown DAG"
    task_id = (
        context["task_instance"].task_id
        if context.get("task_instance")
        else "Unknown Task"
    )
    execution_date = context.get("execution_date", "Unknown Execution Date")
    log_url = (
        context["task_instance"].log_url
        if context.get("task_instance")
        else "No Log URL Available"
    )

    retry_message = (
        f"*🗂️ DAG ID*: `{dag_id}`\n"
        f"*📌 Task ID*: `{task_id}`\n"
        f"*♻️ Status*: `Retried`\n"
        f"*📅 Execution Date*: `{execution_date}`\n"
        f"📄 *Log URL*: [View Logs]({log_url})\n\n"
    )

    send_telegram_message(
        message=retry_message, context=context, task_id="send_retry_message_task"
    )


def send_telegram_success_message(context):
    """
    Sends a success notification via Telegram.
    """

    dag_id = context["dag"].dag_id if context.get("dag") else "Unknown DAG"
    task_id = (
        context["task_instance"].task_id
        if context.get("task_instance")
        else "Unknown Task"
    )
    execution_date = context.get("execution_date", "Unknown Execution Date")
    log_url = (
        context["task_instance"].log_url
        if context.get("task_instance")
        else "No Log URL Available"
    )

    success_message = (
        f"*🗂️ DAG ID*: `{dag_id}`\n"
        f"*📌 Task ID*: `{task_id}`\n"
        f"*✅ Status*: Success\n"
        f"*📅 Execution Date*: `{execution_date}`\n"
        f"📄 *Log URL*: [View Logs]({log_url})\n\n"
    )

    send_telegram_message(
        message=success_message, context=context, task_id="send_success_message_task"
    )
