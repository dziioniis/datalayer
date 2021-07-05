package com.lambda.collect.datalayer.types;

import java.util.Date;

public class PgTimestamp extends ColumnType {
    @Override
    public String getDbName() {
        return "timestamp without time zone";
    }

    @Override
    public Class<?> getClassType() {
        return Date.class;
    }
}
