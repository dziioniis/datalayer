package com.lambda.collect.datalayer.types;

public class PgTime extends ColumnType {
    @Override
    public String getDbName() {
        return "time without time zone";
    }

    @Override
    public Class<?> getClassType() {
        return String.class;
    }
}
