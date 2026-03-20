import logging
import json
from datetime import datetime


class PipelineLogger:
    """Structured JSON logger for pipeline observability."""

    def __init__(self, dag_id: str, task_id: str, run_id: str):
        self.context = {
            'dag_id': dag_id,
            'task_id': task_id,
            'run_id': run_id
        }
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f'{dag_id}.{task_id}')

    def info(self, message: str, **kwargs):
        self._log('INFO', message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._log('WARNING', message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log('ERROR', message, **kwargs)

    def metric(self, metric_name: str, value, **kwargs):
        self._log('METRIC', metric_name, value=value, **kwargs)

    def _log(self, level: str, message: str, **kwargs):
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': level,
            'message': message,
            **self.context,
            **kwargs
        }
        self.logger.info(json.dumps(entry))