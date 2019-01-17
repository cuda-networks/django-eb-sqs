from __future__ import absolute_import, unicode_literals

from contextlib import contextmanager

from django.db import reset_queries, close_old_connections


@contextmanager
def django_db_management():
    # type: () -> None
    reset_queries()
    close_old_connections()
    try:
        yield
    finally:
        close_old_connections()
