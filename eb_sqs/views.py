from __future__ import absolute_import, unicode_literals

from django.http import HttpResponseServerError, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from eb_sqs.worker.worker_exceptions import ExecutionFailedException
from eb_sqs.worker.worker_exceptions import InvalidMessageFormatException
from eb_sqs.worker.worker_factory import WorkerFactory


@require_http_methods(['POST'])
@csrf_exempt
def process_task(request):
    # type: (HttpRequest) -> HttpResponse
    try:
        worker = WorkerFactory.default().create()
        worker.execute(request.body)
        return HttpResponse(status=200)
    except InvalidMessageFormatException:
        return HttpResponse(status=400)
    except ExecutionFailedException:
        return HttpResponseServerError()
