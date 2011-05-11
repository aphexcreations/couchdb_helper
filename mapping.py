"""
Enhanced ViewField
"""

import dateutil.parser

from couchdb.mapping import DEFAULT
from couchdb.mapping import Field, ViewField

from design import EnhancedViewDefinition


def V(design, name, reduce=None):
    """
    Shortcut for ViewField and EnhancedViewField

    @author BrendonCrawford
    """
    if reduce is None:
        return ViewField.define(design=design, name=name)
    else:
        return EnhancedViewField.define(design=design,
                                        name=name,
                                        reduce=reduce)


class EnhancedDateTimeField(Field):
    """
    Improves on the standard DateTimeField which doesnt correctly
    handle UTC
    """

    def _to_python(self, value):
        if isinstance(value, basestring):
            value = dateutil.parser.parse(value)
        else:
            value = None
        return value

    def _to_json(self, value):
        return value.isoformat()


class EnhancedViewField(ViewField):
    """
    Enhanced viewfield
    Offers improcements over the standard couchdb viewfield such as support
    for reduce functions in decorators
    """

    @classmethod
    def define(cls, design, name=None,
               language='python',
               wrapper=DEFAULT, reduce=None, **defaults):
        """
        Allows for decorators with reduce function

        Factory method for use as a decorator (only suitable for Python
        view code).

        @author BrendonCrawford
        @param cls Class
        @param design String
        @param name String
        @param language String
        @param wrapper Function
        @param reduce Function|Method
        @param defaults Dict
        """
        def view_wrapped(fun):
            return cls(design, fun, reduce_fun=reduce,
                       language=language, wrapper=wrapper,
                       **defaults)
        return view_wrapped

    def __get__(self, instance, cls=None):
        """
        Override for __get__

        @author BrendonCrawford
        @param instance
        @param cls
        """
        if self.wrapper is DEFAULT:
            wrapper = cls._wrap_row
        else:
            wrapper = self.wrapper
        return EnhancedViewDefinition(self.design, self.name, self.map_fun,
                                      self.reduce_fun, language=self.language,
                                      wrapper=wrapper, **self.defaults)
