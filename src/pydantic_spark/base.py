from pydantic import BaseModel


class SparkBase(BaseModel):
    """This is base pydantic class that will add some methods"""

    @classmethod
    def spark_schema(cls) -> dict:
        """Return the avro schema for the pydantic class"""
        schema = cls.schema()
        return cls._spark_schema(schema)

    @staticmethod
    def _spark_schema(schema: dict) -> dict:
        """Return the spark schema for the given pydantic schema"""
        return {}
        raise NotImplementedError
