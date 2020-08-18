class PipelineException(Exception):
    pass


class CycleException(PipelineException):
    pass


class KeyAlreadyExists(PipelineException):
    pass


class DoubleProvideException(PipelineException):
    pass


class NoProvide(PipelineException):
    pass
