from pydantic import BaseModel

class SparkBase(BaseModel):
    @classmethod
    def spark_schema(cls) -> dict: ...
