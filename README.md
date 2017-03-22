## Django EB SQS - Background Tasks for Elastic Beanstalk and Amazon SQS

django-eb-sqs is a simple task manager for the Elastic Beanstalk Worker Tier. It uses SQS and the [boto3](https://github.com/boto/boto3) library.

### Installation

Install the module with `pip install git+git://github.com/cuda-networks/django-eb-sqs.git` or add it to your `requirements.txt`.

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

@task(queue_name='test')
def echo(message):
    print message

echo.delay(message='Hello World!')
```
**NOTE:** This assumes that you have your AWS keys in the appropriate environment variables, or are using IAM roles. Consult the `boto3` [documentation](https://boto3.readthedocs.org/en/latest/) for further info.

If you don't pass a queue name, the `EB_SQS_DEFAULT_QUEUE` setting is used. If not set, the queue name is `default`.

Additionally the task decorator supports `max_retries` (default `0`) and `use_pickle` (default `False`) attributes for advanced control task execution.

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

@task(queue_name='test', max_retries=5)
def upload_file(message):
    print '# of retries: {}'.format(upload_file.retry_num)
    try:
        # upload ...
    expect ConnectionException:
        upload_file.retry()
```

The retry call supports the `delay` and `execute_inline` arguments in order to delay the retry or execute it inline. If the retry shall not be counted for the max retry limit set `count_retries` to false. Use 'retry_num' to get the number of retries for the current task.

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

```bash
python manage.py run_eb_sqs_worker --url <absoulte endpoint url> --queue <queue-name>
```

For example:

```bash
python manage.py run_eb_sqs_worker --url http://127.0.0.1:80/worker/process --queue default
```

#### Executing Tasks without Elastic Beanstalk

Another way of executing tasks is to use the Django command `process_queue`.
This command can work with one or more queues, reading from the queues infinitely and executing tasks as they come-in.

```bash
python manage.py process_queue --queues <comma-delimited list of queue names>
```

This is a good idea for someone who wants to execute tasks without an Elastic Beanstalk worker.


#### Group Tasks
Multiple tasks can be grouped by specifing the `group_id` argument when calling `delay` on a task.
If all tasks of a specific group are executed then the group callback task specified by `EB_SQS_GROUP_CALLBACK_TASK` is executed.

Example calls:
```python
echo.delay(message='Hello World!', group_id='1')
echo.delay(message='Hallo Welt!', group_id='1')
echo.delay(message='Hola mundo!', group_id='1')
```

Example callback which is executed when all three tasks are finished:
```python
from eb_sqs.decorators import task

@task(queue_name='test', max_retries=5)
def group_finished(group_id):
    pass
```

#### Settings

The following settings can be used to fine tune django-eb-sqs. Copy them into your Django `settings.py` file.

- EB_AWS_REGION (`us-east-1`): The AWS region to use when working with SQS.
- EB_SQS_MAX_NUMBER_OF_MESSAGES (`10`): The maximum number of messages to read in a single call from SQS (<= 10).
- EB_SQS_WAIT_TIME_S (`2`): The time to wait (seconds) when receiving messages from SQS.
- EB_SQS_AUTO_ADD_QUEUE (`True`): If queues should be added automatically to AWS if they don't exist.
- EB_SQS_DEAD_LETTER_MODE (`False`): Enable if this worker is handling the SQS dead letter queue. Tasks won't be executed but group callback is.
- EB_SQS_DEFAULT_DELAY (`0`): Default task delay time in seconds.
- EB_SQS_DEFAULT_MAX_RETRIES (`0`): Default retry limit for all tasks.
- EB_SQS_DEFAULT_COUNT_RETRIES (`True`): Count retry calls. Needed if max retries check shall be executed.
- EB_SQS_DEFAULT_QUEUE (`default`): Default queue name if none is specified when creating a task.
- EB_SQS_EXECUTE_INLINE (`False`): Execute tasks immediately without using SQS. Useful during development.
- EB_SQS_FORCE_SERIALIZATION (`False`): Forces serialization of tasks when executed `inline`. This setting is helpful during development to see if all arguments are serialized and deserialized properly.
- EB_SQS_GROUP_CALLBACK_TASK (`None`): Group callback (String or Function). Must be a valid task.
- EB_SQS_QUEUE_PREFIX (`eb-sqs-`): Prefix to use for the queues. The prefix is added to the queue name.
- EB_SQS_REDIS_CLIENT (`None`): Set the Redis connection client (`StrictRedis`)
- EB_SQS_REDIS_EXPIRY (`604800`): Default expiry time in seconds until a group is removed
- EB_SQS_REDIS_KEY_PREFIX (`eb-sqs-`): Prefix used for all Redis keys
- EB_SQS_USE_PICKLE (`False`): Enable to use `pickle` to serialize task parameters. Uses `json` as default.


### Development

Make sure to install the development dependencies from `development.txt`.

#### Tests

The build in tests can be executed with the Django test runner.

```bash
django-admin test --settings=eb_sqs.test_settings
```
