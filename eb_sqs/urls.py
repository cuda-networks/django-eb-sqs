from __future__ import absolute_import, unicode_literals

from django.conf.urls import url

from eb_sqs.views import process_task

app_name = 'eb_sqs'
urlpatterns = [
    url(r'^process$', process_task, name='process_task'),
]
