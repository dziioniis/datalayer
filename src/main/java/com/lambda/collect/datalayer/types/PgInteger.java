package com.lambda.collect.datalayer.types;

public class PgInteger extends ColumnType {
    @Override
    public String getDbName() {
        return "INT";
    }

    @Override
    public Class<?> getClassType() {
        return int.class;
    }
}
