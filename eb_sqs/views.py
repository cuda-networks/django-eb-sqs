from django.http import HttpResponse
from django.views.decorators.http import require_http_methods
from eb_sqs.worker import Worker


@require_http_methods(['POST'])
def process_task(request):
    worker = Worker()

    try:
        worker.execute(request.body)
        return HttpResponse(status=200)
    except Worker.InvalidMessageFormat:
        return HttpResponse(status=200)
    except Worker.ExecutionFailedException:
        return HttpResponse(status=500)
