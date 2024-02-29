from __future__ import annotations

import json
from datetime import date, datetime
from enum import Enum
from typing import Dict, List, Optional, Union
from uuid import UUID

from pydantic import Field
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pydantic_spark.base import CoerceType, SparkBase


class Nested2Model(SparkBase):
    c111: str


class NestedModel(SparkBase):
    c11: Nested2Model


class ModelTest(SparkBase):
    c1: str
    c2: int
    c3: float
    c4: datetime
    c5: date
    c6: Optional[str]
    c7: bool
    c8: UUID
    c9: Optional[UUID]
    c10: Dict[str, str]
    c11: dict


class ComplexTestModel(SparkBase):
    c1: List[str]
    c2: NestedModel
    c3: List[NestedModel]
    c4: List[datetime]
    c5: Dict[str, NestedModel]


class ReusedObject(SparkBase):
    c1: Nested2Model
    c2: Nested2Model


class ReusedObjectArray(SparkBase):
    c1: List[Nested2Model]
    c2: Nested2Model


class DefaultValues(SparkBase):
    c1: str = "test"


def test_spark():
    expected_schema = StructType(
        [
            StructField(
                "c1",
                StringType(),
                nullable=False,
                metadata={"parentClass": "ModelTest"},
            ),
            StructField("c2", LongType(), nullable=False, metadata={"parentClass": "ModelTest"}),
            StructField(
                "c3",
                DoubleType(),
                nullable=False,
                metadata={"parentClass": "ModelTest"},
            ),
            StructField(
                "c4",
                TimestampType(),
                nullable=False,
                metadata={"parentClass": "ModelTest"},
            ),
            StructField("c5", DateType(), nullable=False, metadata={"parentClass": "ModelTest"}),
            StructField("c6", StringType(), nullable=True, metadata={"parentClass": "ModelTest"}),
            StructField(
                "c7",
                BooleanType(),
                nullable=False,
                metadata={"parentClass": "ModelTest"},
            ),
            StructField(
                "c8",
                StringType(),
                nullable=False,
                metadata={"logicalType": "uuid", "parentClass": "ModelTest"},
            ),
            StructField(
                "c9",
                StringType(),
                nullable=True,
                metadata={"logicalType": "uuid", "parentClass": "ModelTest"},
            ),
            StructField(
                "c10",
                MapType(StringType(), StringType()),
                nullable=False,
                metadata={"parentClass": "ModelTest"},
            ),
            StructField(
                "c11",
                MapType(StringType(), StringType()),
                nullable=False,
                metadata={"parentClass": "ModelTest"},
            ),
        ]
    )
    result = ModelTest.spark_schema()
    assert result == json.loads(expected_schema.json())
    # Reading schema with spark library to be sure format is correct
    schema = StructType.fromJson(result)
    assert len(schema.fields) == 11


def test_reused_object():
    expected_schema = StructType(
        [
            StructField(
                "c1",
                StructType.fromJson(Nested2Model.spark_schema()),
                nullable=False,
                metadata={"parentClass": "ReusedObject"},
            ),
            StructField(
                "c2",
                StructType.fromJson(Nested2Model.spark_schema()),
                nullable=False,
                metadata={"parentClass": "ReusedObject"},
            ),
        ]
    )
    result = ReusedObject.spark_schema()
    assert result == json.loads(expected_schema.json())
    # Reading schema with spark library to be sure format is correct
    schema = StructType.fromJson(result)
    assert len(schema.fields) == 2


def test_reused_object_array():
    expected_schema = StructType(
        [
            StructField(
                "c1",
                ArrayType(StructType.fromJson(Nested2Model.spark_schema())),
                nullable=False,
                metadata={"parentClass": "ReusedObjectArray"},
            ),
            StructField(
                "c2",
                StructType.fromJson(Nested2Model.spark_schema()),
                nullable=False,
                metadata={"parentClass": "ReusedObjectArray"},
            ),
        ]
    )
    result = ReusedObjectArray.spark_schema()
    assert result == json.loads(expected_schema.json())
    # Reading schema with spark library to be sure format is correct
    schema = StructType.fromJson(result)
    assert len(schema.fields) == 2


def test_complex_spark():
    expected_schema = StructType(
        [
            StructField(
                "c1",
                ArrayType(StringType()),
                nullable=False,
                metadata={"parentClass": "ComplexTestModel"},
            ),
            StructField(
                "c2",
                StructType.fromJson(NestedModel.spark_schema()),
                nullable=False,
                metadata={"parentClass": "ComplexTestModel"},
            ),
            StructField(
                "c3",
                ArrayType(StructType.fromJson(NestedModel.spark_schema())),
                nullable=False,
                metadata={"parentClass": "ComplexTestModel"},
            ),
            StructField(
                "c4",
                ArrayType(TimestampType()),
                nullable=False,
                metadata={"parentClass": "ComplexTestModel"},
            ),
            StructField(
                "c5",
                MapType(StringType(), StructType.fromJson(NestedModel.spark_schema())),
                nullable=False,
                metadata={"parentClass": "ComplexTestModel"},
            ),
        ]
    )
    result = ComplexTestModel.spark_schema()
    assert result == json.loads(expected_schema.json())
    # Reading schema with spark library to be sure format is correct
    schema = StructType.fromJson(result)
    assert len(schema.fields) == 5


def test_defaults():
    expected_schema = StructType(
        [
            StructField(
                "c1",
                StringType(),
                nullable=False,
                metadata={"parentClass": "DefaultValues", "default": "test"},
            )
        ]
    )
    result = DefaultValues.spark_schema()
    assert result == json.loads(expected_schema.json())
    # Reading schema with spark library to be sure format is correct
    schema = StructType.fromJson(result)
    assert len(schema.fields) == 1


class StringEnumValue(str, Enum):
    v1 = "v1"
    v2 = "v2"


class IntEnumValue(int, Enum):
    v1 = 1
    v2 = 2


class FloatEnumValue(float, Enum):
    v1 = 1.1
    v2 = 2.2


class EnumTest(SparkBase):
    c1: StringEnumValue
    c2: IntEnumValue
    c3: FloatEnumValue
    c4: StringEnumValue = Field(default=StringEnumValue.v1)


def test_enum():
    expected_schema = StructType(
        [
            StructField("c1", StringType(), nullable=False, metadata={"parentClass": "EnumTest"}),
            StructField("c2", LongType(), nullable=False, metadata={"parentClass": "EnumTest"}),
            StructField("c3", DoubleType(), nullable=False, metadata={"parentClass": "EnumTest"}),
            StructField(
                "c4",
                StringType(),
                nullable=False,
                metadata={"parentClass": "EnumTest", "default": StringEnumValue.v1},
            ),
        ]
    )
    result = EnumTest.spark_schema()
    assert result == json.loads(expected_schema.json())


def test_coerce_type():
    class TestCoerceType(SparkBase):
        c1: int = Field(json_schema_extra={"coerce_type": CoerceType.integer})
        c2: Union[str, int] = Field(json_schema_extra={"coerce_type": CoerceType.string})

    result = TestCoerceType.spark_schema()
    assert result["fields"][0]["type"] == "integer"
    assert result["fields"][1]["type"] == "string"


class Nested2ModelCoerceType(SparkBase):
    c111: str = Field(json_schema_extra={"coerce_type": CoerceType.integer})


class NestedModelCoerceType(SparkBase):
    c11: Nested2ModelCoerceType


class ComplexTestModelCoerceType(SparkBase):
    c1: List[NestedModelCoerceType]


def test_coerce_type_complex_spark():
    expected_schema = StructType(
        [
            StructField(
                "c1",
                ArrayType(StructType.fromJson(NestedModelCoerceType.spark_schema())),
                nullable=False,
                metadata={"parentClass": "ComplexTestModelCoerceType"},
            )
        ]
    )
    result = ComplexTestModelCoerceType.spark_schema()
    assert result == json.loads(expected_schema.json())
    # Reading schema with spark library to be sure format is correct
    schema = StructType.fromJson(result)
    assert len(schema.fields) == 1
    assert isinstance(
        schema.fields[0].dataType.elementType.fields[0].dataType.fields[0].dataType,
        IntegerType,
    )
