class RetryableTaskException(Exception):
    def __init__(self, inner):
        # type: (Exception) -> None
        self._inner = inner

    def __repr__(self):
        # type: () -> str
        return repr(self._inner)
