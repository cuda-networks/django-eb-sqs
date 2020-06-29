
[![PyPI version](https://img.shields.io/pypi/v/django-eb-sqs)](https://pypi.org/project/django-eb-sqs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CircleCI](https://img.shields.io/circleci/build/github/cuda-networks/django-eb-sqs/master)](https://circleci.com/gh/cuda-networks/django-eb-sqs/tree/master)

# Django EB SQS - Background Tasks for Amazon SQS

django-eb-sqs is a simple task manager for AWS SQS. It uses SQS and the [boto3](https://github.com/boto/boto3) library.

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
    print(message)

echo.delay(message='Hello World!')
```
**NOTE:** This assumes that you have your AWS keys in the appropriate environment variables, or are using IAM roles. Consult the `boto3` [documentation](https://boto3.readthedocs.org/en/latest/) for further info.

If you don't pass a queue name, the `EB_SQS_DEFAULT_QUEUE` setting is used. If not set, the queue name is `eb-sqs-default`.

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
    print('# of retries: {}'.format(upload_file.retry_num))
    try:
        # upload ...
    except ConnectionException:
        upload_file.retry()
```

The retry call supports the `delay` and `execute_inline` arguments in order to delay the retry or execute it inline. If the retry shall not be counted for the max retry limit set `count_retries` to false. Use 'retry_num' to get the number of retries for the current task.

**NOTE:** `retry()` throws a `MaxRetriesReachedException` exception if the maximum number of retries is reached.

#### Executing Tasks

In order to execute tasks, use the Django command `process_queue`.
This command can work with one or more queues, reading from the queues infinitely and executing tasks as they come-in.

```bash
python manage.py process_queue --queues <comma-delimited list of queue names>
```

You can either use full queue names, or queue prefix using `prefix:*my_example_prefix*` notation.

Examples:
```bash
python manage.py process_queue --queues queue1,queue2 # process queue1 and queue2
python manage.py process_queue --queues queue1,prefix:pr1-,queue2 # process queue1, queue2 and any queue whose name starts with 'pr1-'
```

Use the signals `MESSAGES_RECEIVED`, `MESSAGES_PROCESSED`, `MESSAGES_DELETED` of the `WorkerService` to get informed about the current SQS batch being processed by the management command.

#### Auto Tasks

This is a helper tool for the case you wish to define one of your class method as a task, and make it seamless to all callers.
This makes the code much simpler, and allows using classes to invoke your method directly without considering whether it's invoked async or not.

This is how you would define your class:
```python
from eb_sqs.auto_tasks.service import AutoTaskService

class MyService:
    def __init__(self, p1=default1, ..., pN=defaultN, auto_task_service=None):
        self._auto_task_service = auto_task_service or AutoTaskService()

        self._auto_task_service.register_task(self.my_task_method)
    
    def my_task_method(self, *args, **kwargs):
        ...

```

Notice the following:
1. Your class needs to have defaults for all parameters in the c'tor
2. The c'tor must have a parameter named `auto_task_service`
3. The method shouldn't have any return value (as it's invoked async)

In case you want your method to retry certain cases, you need to raise `RetryableTaskException`.
You can provide on optional `delay` time for the retry, set `count_retries=False` in case you don't want to limit retries, or use `max_retries_func` to specify a function which will be invoked when the defined maximum number of retries is exhausted.   

#### Settings

The following settings can be used to fine tune django-eb-sqs. Copy them into your Django `settings.py` file.

- EB_AWS_REGION (`us-east-1`): The AWS region to use when working with SQS.
- EB_SQS_MAX_NUMBER_OF_MESSAGES (`10`): The maximum number of messages to read in a single call from SQS (<= 10).
- EB_SQS_WAIT_TIME_S (`2`): The time to wait (seconds) when receiving messages from SQS.
- NO_QUEUES_WAIT_TIME_S (`5`): The time a workers waits if there are no SQS queues available to process.
- EB_SQS_AUTO_ADD_QUEUE (`False`): If queues should be added automatically to AWS if they don't exist.
- EB_SQS_QUEUE_MESSAGE_RETENTION (`1209600`): The value (in seconds) to be passed to MessageRetentionPeriod parameter, when creating a queue (only relevant in case EB_SQS_AUTO_ADD_QUEUE is set to True).
- EB_SQS_QUEUE_VISIBILITY_TIMEOUT (`300`): The value (in seconds) to be passed to VisibilityTimeout parameter, when creating a queue (only relevant in case EB_SQS_AUTO_ADD_QUEUE is set to True).
- EB_SQS_DEAD_LETTER_MODE (`False`): Enable if this worker is handling the SQS dead letter queue. Tasks won't be executed but group callback is.
- EB_SQS_DEFAULT_DELAY (`0`): Default task delay time in seconds.
- EB_SQS_DEFAULT_MAX_RETRIES (`0`): Default retry limit for all tasks.
- EB_SQS_DEFAULT_COUNT_RETRIES (`True`): Count retry calls. Needed if max retries check shall be executed.
- EB_SQS_DEFAULT_QUEUE (`eb-sqs-default`): Default queue name if none is specified when creating a task.
- EB_SQS_EXECUTE_INLINE (`False`): Execute tasks immediately without using SQS. Useful during development. Global setting `True` will override setting it on a task level.
- EB_SQS_FORCE_SERIALIZATION (`False`): Forces serialization of tasks when executed `inline`. This setting is helpful during development to see if all arguments are serialized and deserialized properly.
- EB_SQS_QUEUE_PREFIX (``): Prefix to use for the queues. The prefix is added to the queue name.
- EB_SQS_USE_PICKLE (`False`): Enable to use `pickle` to serialize task parameters. Uses `json` as default.
- EB_SQS_AWS_MAX_RETRIES (`30`): Default retry limit on a boto3 call to AWS SQS.
- EB_SQS_REFRESH_PREFIX_QUEUES_S (`10`): Minimal number of seconds to wait between refreshing queue list, in case prefix is used


### Development

Make sure to install the development dependencies from `development.txt`.

#### Tests

The build in tests can be executed with the Django test runner.

```bash
python -m django test --settings=eb_sqs.test_settings
```
