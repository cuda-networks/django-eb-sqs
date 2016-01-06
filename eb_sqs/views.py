from django.http import JsonResponse, HttpResponseServerError
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from eb_sqs.worker import Worker


@require_http_methods(['POST'])
@csrf_exempt
def process_task(request):
    worker = Worker()

    try:
        worker.execute(request.body)
        return JsonResponse(data={})
    except Worker.InvalidMessageFormat:
        return JsonResponse(data={})
    except Worker.ExecutionFailedException:
        return HttpResponseServerError()
