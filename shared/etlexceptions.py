import pprint


class BaseExpTaskException(Exception):
    pass


class GPExpTaskException(BaseExpTaskException):
    def __init__(self, message, errors={}):
        super(BaseExpTaskException, self).__init__(message)
        self._errors = errors

    def __repr__(self):
        """Return prints of the exception information.
        :return:
        """
        pprint.pprint(self._errors)
        return "{}".format(self._errors)


class ETLException(Exception):
    def __init__(self, *args, **kwargs):
        super(ETLException, self).__init__(args, kwargs)


class ETLExceptionWithContext(Exception):
    def __init___(self, errargs):
        errmsg = "puff_exception raised with : {}".format(errargs)
        super(ETLExceptionWithContext, self).__init__(errargs)
        self._errmsg = errmsg
