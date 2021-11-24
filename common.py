"""A one line summary of the module or program, terminated by a period.

Leave one blank line.  The rest of this docstring should contain an
overall description of the module or program.  Optionally, it may also
contain a brief description of exported classes and functions and/or usage
examples.

  Typical usage example:

  foo = ClassFoo()
  bar = foo.FunctionBar()
"""

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
