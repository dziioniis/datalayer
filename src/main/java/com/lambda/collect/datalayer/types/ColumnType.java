package com.lambda.collect.datalayer.types;

public abstract class ColumnType {
    String dbName;
    Class<?> classType;

    public abstract String getDbName();

    public abstract Class<?> getClassType();
}
