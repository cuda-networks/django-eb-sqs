class RetryableTaskException(Exception):
    def __init__(self, inner: Exception, delay: int = None, count_retries: bool = None, max_retries_func=None):
        self._inner = inner

        self.delay = delay
        self.count_retries = count_retries
        self.max_retries_func = max_retries_func

    def __repr__(self) -> str:
        return repr(self._inner)
