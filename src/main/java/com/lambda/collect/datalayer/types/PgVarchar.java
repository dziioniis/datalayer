package com.lambda.collect.datalayer.types;

public class PgVarchar extends ColumnType {
    private final int length;

    public PgVarchar(int stringLength) {
        this.length = stringLength;
    }

    @Override
    public String getDbName() {
        return "character varying(" + length + ")";
    }

    @Override
    public Class<?> getClassType() {
        return String.class;
    }
}
