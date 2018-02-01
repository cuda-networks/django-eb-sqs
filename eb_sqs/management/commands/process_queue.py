from __future__ import absolute_import, unicode_literals

from django.core.management import BaseCommand, CommandError

from eb_sqs.worker.service import WorkerService


class Command(BaseCommand):
    help = 'Command to process tasks from one or more SQS queues'

    def add_arguments(self, parser):
        parser.add_argument('--queues', '-q',
                            dest='queue_names',
                            help='Name of queues to process, separated by commas')

    def handle(self, *args, **options):
        if not options['queue_names']:
            raise CommandError('Queue names (--queues) not specified')

        queue_names = [queue_name.rstrip() for queue_name in options['queue_names'].split(',')]

        WorkerService().process_queues(queue_names)
