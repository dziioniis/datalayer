package com.lambda.collect.datalayer.types;

public class PgText extends ColumnType {
    @Override
    public String getDbName() {
        return "TEXT";
    }

    @Override
    public Class<?> getClassType() {
        return String.class;
    }
}
