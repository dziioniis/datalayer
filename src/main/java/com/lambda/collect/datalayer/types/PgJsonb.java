package com.lambda.collect.datalayer.types;

public class PgJsonb extends ColumnType {
    @Override
    public String getDbName() {
        return "JSONB";
    }

    @Override
    public Class<?> getClassType() {
        return String.class;
    }
}
