package com.lambda.collect.datalayer.utils;

public class SequenceAggregator {
    private final StringBuilder create = new StringBuilder("CREATE SEQUENCE IF NOT EXISTS ");
    private final StringBuilder increment = new StringBuilder();
    private final StringBuilder minValue = new StringBuilder();
    private final StringBuilder maxValue = new StringBuilder();
    private final StringBuilder start = new StringBuilder();
    private final StringBuilder owned = new StringBuilder();


    public void appendName(String sequenceName) {
        this.create.append(sequenceName);
    }

    public void appendIncrement(long increment) {
        this.increment.append(" INCREMENT BY ").append(increment);
    }

    public void appendMinValue(long minValue) {
        this.minValue.append(" MINVALUE ").append(minValue);
    }

    public void appendMaxValue(long maxValue) {
        this.maxValue.append(" MAXVALUE ").append(maxValue);
    }

    public void appendStartWith(long startValue) {
        this.start.append(" START WITH ").append(startValue);
    }

    public void appendOwnedBy(String tableName, String columnName) {
        this.owned.append(" OWNED BY ").append(tableName).append(".").append(columnName);
    }

    public String buildQuery() {
        return create
                .append(increment)
                .append(minValue)
                .append(maxValue)
                .append(start)
                .append(owned).append(";").toString();
    }
}
