from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from typing import List, Sequence


def flatten_schema(df: DataFrame) -> DataFrame:
    def _extract_flat_prefixes(prefix: List[str], data_type: StructType) -> List[List[str]]:
        columns: List[List[str]] = []
        for column in data_type:
            new_prefix = prefix + [column.name]
            if isinstance(column.dataType, StructType):
                nested_columns = _extract_flat_prefixes(new_prefix, column.dataType) 
                columns.extend(nested_columns)
            else:
                columns.append(new_prefix)
        
        return columns
    
    prefixes = _extract_flat_prefixes([], df.schema)
    return df.select(*[F.col(".".join(prefix)).alias("__".join(prefix)) for prefix in prefixes])

def union_all(dfs: Sequence[DataFrame]) -> DataFrame:
    return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)

def ceil_division(a: int, b: int) -> int:
    return (a + b - 1) // b

