"""Common."""

UUID = str
Timestamp = int


class UnknownEdgeLinks(Exception):
    """ Raised when creating edge with invalid src and/or dst ids  """

    def __init__(self, prefix: str):
        full_message = f'{prefix} unknown src and/or dst for edge'
        super().__init__(full_message)


class NotFound(Exception):
    """ Raised when link or edge lookup fails """

    def __init__(self, prefix: str):
        full_message = f'{prefix} not found'
        super().__init__(full_message)
