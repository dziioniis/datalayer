package com.lambda.collect.datalayer.types;

public class PgBigDecimal extends ColumnType {
    private final int precision;
    private final int scale;

    public PgBigDecimal(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public String getDbName() {
        return "numeric(" + precision + "," + scale + ")";
    }

    @Override
    public Class<?> getClassType() {
        return String.class;
    }
}
