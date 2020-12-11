class PipelineException(Exception):
    pass


class CycleException(PipelineException):
    pass


class DoubleProvideException(PipelineException):
    pass
