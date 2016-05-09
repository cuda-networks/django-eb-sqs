from __future__ import absolute_import, unicode_literals

from django.http import HttpResponseServerError, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods

from eb_sqs.worker import Worker


@require_http_methods(['POST'])
@csrf_exempt
def process_task(request):
    worker = Worker()

    try:
        worker.execute(request.body)
        return HttpResponse(status=200)
    except Worker.InvalidMessageFormat:
        return HttpResponse(status=400)
    except Worker.ExecutionFailedException:
        return HttpResponseServerError()
