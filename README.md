## Django EB SQS - Background Tasks for Elastic Beanstalk and Amazon SQS

django-eb-sqs is a simple task manager for the Elastic Beanstalk Worker Tier. It uses SQS and the [boto3](https://github.com/boto/boto3) library.

### Installation

Install the module with `pip install django-eb-sqs` or add it to your `requirements.txt`.

Don't forget to add django-eb-sqs app to your Django `INSTALLED_APPS` settings:
```python
INSTALLED_APPS = (
    ...,
    'eb_sqs',
)
```

### Usage

#### Creating Tasks

Adding a task to a queue is simple.

```python
from eb_sqs.decorators import task

@task(queue='test')
def echo(message):
    print message

echo.delay(message='Hello World!')
```
**NOTE:** This assumes that you have your AWS keys in the appropriate environment variables, or are using IAM roles. Consult the `boto3` [documentation](https://boto3.readthedocs.org/en/latest/) for further info.

If you don't pass a queue name, the `EB_SQS_DEFAULT_QUEUE` setting is used. If not set, the queue name is `default`.

You can also delay the execution of a task by specifying the delay time in seconds.

```python
echo.delay(message='Hello World!', delay=60)
```

During development it is sometimes useful to execute a task immediately without using SQS. This is possible with the `execute_inline` argument.

```python
echo.delay(message='Hello World!', execute_inline=True)
```

**NOTE:** `delay` is not applied when `execute_inline` is set to `True`.

Failed tasks can be retried by using the `retry` method. See the following example:

```python
from eb_sqs.decorators import task

@task(queue='test', max_retries=5)
def upload_file(message):
    try:
        # upload ...
    expect ConnectionException:
        upload_file.retry()
```

The retry call supports the `delay` and `execute_inline` arguments in order to delay the retry or execute it inline.

**NOTE:** `retry()` throws a `MaxRetriesReachedException` exception if the maximum number of retries is reached.

#### Executing Tasks

The Elastic Beanstalk Worker Tier sends all tasks to a API endpoint. django-eb-sqs has already such an endpoint which can be used by specifing the url mapping in your `urls.py` file.

```python
urlpatterns = [
    ...
    url(r'^worker/', include('eb_sqs.urls', namespace='eb_sqs'))
]
```

In that case the relative endpoint url would be: `worker/process`

Set this url in the Elastic Beanstalk Worker settings prior to deployment.

During development you can use the included Django command to execute a small script which retrieves messages from SQS and posts them to this endpoint.

```python
python manage.py run_eb_sqs_worker --url <absoulte endpoint url> --queue <queue-name>
```

For example:

```python
python manage.py run_eb_sqs_worker --url http://127.0.0.1:80/worker/process --queue default
```


#### Settings

The following settings can be used to fine tune django-eb-sqs. Copy them into your Django `settings.py` file.

- EB_SQS_AUTO_ADD_QUEUE (`True`): If queues should be added automatically to AWS if they don't exist.
- EB_SQS_QUEUE_PREFIX (`eb-sqs-`): Prefix to use for the queues. The prefix is added to the queue name.
- EB_SQS_DEFAULT_QUEUE (`default`): Default queue name if none is specified when creating a task.
- EB_SQS_EXECUTE_INLINE (`False`): Execute tasks immediately without using SQS. Useful during development.
- EB_SQS_DEFAULT_DELAY (`0`): Default task delay time in seconds.
- EB_SQS_DEFAULT_MAX_RETRIES (`0`): Default retry limit for all tasks.

### Development

Make sure to install the development dependencies from `development.txt`.

#### Tests

The build in tests can be executed with the Django test runner.

```python
django-admin test --settings=eb_sqs.test_settings
```
