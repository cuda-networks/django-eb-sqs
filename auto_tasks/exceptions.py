class RetryableTaskException(Exception):
    def __init__(self, inner, delay=None, count_retries=None):
        # type: (Exception, int, bool) -> None
        self._inner = inner

        self.delay = delay
        self.count_retries = count_retries

    def __repr__(self):
        # type: () -> str
        return repr(self._inner)
