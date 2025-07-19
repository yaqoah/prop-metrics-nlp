class ScrapingError(Exception):
    pass

class RateLimitError(ScrapingError):
    def __init__(self, message="Rate limit exceeded", retry_after=None):
        self.retry_after = retry_after
        super().__init__(message)

class ProxyError(ScrapingError):
    pass

class ParseError(ScrapingError):
    pass

class ValidationError(ScrapingError):
    pass

class AuthenticationError(ScrapingError):
    pass

class NetworkError(ScrapingError):
    pass

class DataIntegrityError(ScrapingError):
    pass