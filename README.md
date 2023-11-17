[![Python package](https://github.com/godatadriven/pydantic-spark/actions/workflows/python-package.yml/badge.svg)](https://github.com/godatadriven/pydantic-spark/actions/workflows/python-package.yml)
[![codecov](https://codecov.io/gh/godatadriven/pydantic-spark/branch/main/graph/badge.svg?token=5L08GOERAW)](https://codecov.io/gh/godatadriven/pydantic-spark)
[![PyPI version](https://badge.fury.io/py/pydantic-spark.svg)](https://badge.fury.io/py/pydantic-spark)
[![CodeQL](https://github.com/godatadriven/pydantic-spark/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/godatadriven/pydantic-spark/actions/workflows/codeql-analysis.yml)

# pydantic-spark

This library can convert a pydantic class to a spark schema or generate python code from a spark schema.

### Install

```bash
pip install pydantic-spark
```

### Pydantic class to spark schema

```python
import json
from typing import Optional

from pydantic_spark.base import SparkBase

class TestModel(SparkBase):
    key1: str
    key2: int
    key2: Optional[str]

schema_dict: dict = TestModel.spark_schema()
print(json.dumps(schema_dict))

```
#### Coerce type
Pydantic-spark provides a `coerce_type` option that allows type coercion. 
When applied to a field, pydantic-spark converts the column's data type to the specified coercion type. 

```python
import json
from pydantic import Field
from pydantic_spark.base import SparkBase, CoerceType

class TestModel(SparkBase):
    key1: str = Field(extra_json_schema={"coerce_type": CoerceType.integer})

schema_dict: dict = TestModel.spark_schema()
print(json.dumps(schema_dict))

```


### Install for developers

###### Install package

- Requirement: Poetry 1.*

```shell
poetry install
```

###### Run unit tests
```shell
pytest
coverage run -m pytest  # with coverage
# or (depends on your local env) 
poetry run pytest
poetry run coverage run -m pytest  # with coverage
```

##### Run linting

The linting is checked in the github workflow. To fix and review issues run this:
```shell
black .   # Auto fix all issues
isort .   # Auto fix all issues
pflake .  # Only display issues, fixing is manual
```
