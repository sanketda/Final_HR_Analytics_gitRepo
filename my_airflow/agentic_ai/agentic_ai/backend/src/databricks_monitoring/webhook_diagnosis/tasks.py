from celery_worker import celery
from diagnosis import run_diagnosis

@celery.task(bind=True, name="run_diagnosis_task", acks_late=True, retry_backoff=True)
def run_diagnosis_task(self, notification_payload):
    
    try:

        run_diagnosis(notification_payload)

    except Exception as exc:
        raise self.retry(exc=exc, max_retries=3)
