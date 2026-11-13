class ApplicationError(RuntimeError):
    pass


class InvalidDoi(ApplicationError):
    pass


class DoiNotFound(ApplicationError):
    def __init__(self, cache_status: str):
        super().__init__("doi not found")
        self.cache_status = cache_status


class UpstreamUnavailable(ApplicationError):
    pass


class ProviderUnavailable(RuntimeError):
    pass
