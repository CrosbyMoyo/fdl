# Databricks notebook source
def hash_ddl(columns: list[str], sort_columns:bool=True) -> str: 
    """
    Generates the DDL expression for computing a hash using xxhash64
    """
    if sort_columns:
        columns.sort(key=lambda c: c.split('.')[-1])
    casted_columns = [f"CAST({c} AS STRING)" for c in columns]
    hash_ddl = f"xxhash64({', '.join(casted_columns)})"

    return hash_ddl