import sys

import logging
from datetime import timedelta

from django.core.management import BaseCommand
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from eb_sqs import settings

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Checks the SQS worker is healthy, and if not returns a failure code'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        try:
            with open(settings.HEALTHCHECK_FILE_NAME, 'r') as file:
                last_healthcheck_date_str = file.readlines()[0]

                if parse_datetime(last_healthcheck_date_str) < timezone.now() - timedelta(seconds=settings.HEALTHCHECK_UNHEALTHY_PERIOD_S):
                    self._return_failure()
        except Exception:
            self._return_failure()

    @staticmethod
    def _return_failure():
        logger.warning('[django-eb-sqs] Health check failed')
        sys.exit(1)
