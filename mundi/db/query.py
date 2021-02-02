from abc import ABC

NOT_GIVEN = object()


class Query(ABC):
    """
    Maps function calls into database queries.
    """

    def cursor(self):
        """
        Return a SQLAlchemy cursor.
        """

    def value(self, column: str, ref: str, default=NOT_GIVEN):
        """
        Return scalar value of field or raise ValueError, if not available.
        """
        try:
            method = getattr(self, column + "__value")
        except AttributeError:
            print(f"querying value {column} of {ref}")
        else:
            return method(ref)

    def filter(self, **kwargs):
        """
        Filter query using the given fields.
        """
        print(f"querying {args}, {kwargs} at {column}")
        raise NotImplementedError

    def select(self, fields, query):
        """
        Select the given fields from query.
        """
        raise NotImplementedError
