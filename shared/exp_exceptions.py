import pprint


class BaseExpTaskException(Exception):
    pass


class GPExpTaskException(BaseExpTaskException):
    def __init__(self, message, errors={}):

        # call the base class constructor
        super(BaseExpTaskException, self).__init__(message)
        self._errors = errors

    def __repr__(self):
        """Return prints of the exception information.
        :return:
        """
        pprint.pprint(self._errors)
        return "{}".format(self._errors)
