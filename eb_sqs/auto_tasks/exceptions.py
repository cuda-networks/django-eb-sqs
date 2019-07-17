class RetryableTaskException(Exception):
    def __init__(self, inner, delay=None, count_retries=None, max_retries_func=None):
        # type: (Exception, int, bool, Any) -> None
        self._inner = inner

        self.delay = delay
        self.count_retries = count_retries
        self.max_retries_func = max_retries_func

    def __repr__(self):
        # type: () -> str
        return repr(self._inner)
