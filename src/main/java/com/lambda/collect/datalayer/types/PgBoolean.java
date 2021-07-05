package com.lambda.collect.datalayer.types;

public class PgBoolean extends ColumnType {
    @Override
    public String getDbName() {
        return "boolean";
    }

    @Override
    public Class<?> getClassType() {
        return String.class;
    }
}
