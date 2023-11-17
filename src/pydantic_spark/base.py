from enum import Enum
from typing import List, Tuple

from pydantic import BaseModel


class CoerceType(str, Enum):
    integer = "integer"
    long = "long"
    double = "double"
    string = "string"
    boolean = "boolean"
    date = "date"
    timestamp = "timestamp"


class CoerceTypeError(Exception):
    pass


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
        classes_seen = {}

        def get_definition(ref: str, schema: dict):
            id = ref.replace("#/definitions/", "")
            d = schema.get("definitions", {}).get(id)
            if d is None:
                raise RuntimeError(f"Definition {id} does not exist")
            return d

        def get_type_of_definition(ref: str, schema: dict):
            """Reading definition of base schema for nested structs"""
            d = get_definition(ref, schema)

            if "enum" in d:
                enum_type = d.get("type")
                if enum_type == "string":
                    return "string"
                elif enum_type == "number":
                    return "double"
                elif enum_type == "integer":
                    return "long"
                else:
                    raise RuntimeError(f"Unknown enum type: {enum_type}")
            else:
                return {
                    "type": "struct",
                    "fields": get_fields(d),
                }

        def get_type(value: dict) -> Tuple[str, dict]:
            """Returns a type of single field"""
            t = value.get("type")
            f = value.get("format")
            r = value.get("$ref")
            a = value.get("additionalProperties")
            e = value.get("json_schema_extra", {})
            ft = e.get("coerce_type")
            metadata = {}
            if "default" in value:
                metadata["default"] = value.get("default")
            if r is not None:
                class_name = r.replace("#/definitions/", "")
                if class_name in classes_seen:
                    spark_type = classes_seen[class_name]
                else:
                    spark_type = get_type_of_definition(r, schema)
                    classes_seen[class_name] = spark_type
            elif ft is not None:
                if not isinstance(ft, CoerceType):
                    raise CoerceTypeError("coerce_type must be of type CoerceType")
                spark_type = ft.value
            elif t == "array":
                items = value.get("items")
                tn, metadata = get_type(items)
                spark_type = {
                    "type": "array",
                    "elementType": tn,
                    "containsNull": True,
                }
            elif t == "string" and f == "date-time":
                spark_type = "timestamp"
            elif t == "string" and f == "date":
                spark_type = "date"
            # elif t == "string" and f == "time":  # FIXME: time type in spark does not exist
            #     spark_type = {
            #         "type": "long",
            #         "logicalType": "time-micros",
            #     }
            elif t == "string" and f == "uuid":
                spark_type = "string"
                metadata["logicalType"] = "uuid"
            elif t == "string":
                spark_type = "string"
            elif t == "number":
                spark_type = "double"
            elif t == "integer":
                # integer in python can be a long
                spark_type = "long"
            elif t == "boolean":
                spark_type = "boolean"
            elif t == "object":
                if a is None:
                    value_type = "string"
                else:
                    value_type, m = get_type(a)
                # if isinstance(value_type, dict) and len(value_type) == 1:
                # value_type = value_type.get("type")
                spark_type = {"keyType": "string", "type": "map", "valueContainsNull": True, "valueType": value_type}
            else:
                raise NotImplementedError(
                    f"Type '{t}' not support yet, "
                    f"please report this at https://github.com/godatadriven/pydantic-avro/issues"
                )
            return spark_type, metadata

        def get_fields(s: dict) -> List[dict]:
            """Return a list of fields of a struct"""
            fields = []

            required = s.get("required", [])
            for key, value in s.get("properties", {}).items():
                spark_type, metadata = get_type(value)
                metadata["parentClass"] = s.get("title")
                struct_field = {
                    "name": key,
                    "nullable": "default" not in metadata and key not in required,
                    "metadata": metadata,
                    "type": spark_type,
                }

                fields.append(struct_field)
            return fields

        fields = get_fields(schema)

        return {"fields": fields, "type": "struct"}
