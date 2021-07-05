package com.lambda.collect.datalayer.tables;

public enum JoinStrategy {
    INNER_JOIN("INNER JOIN"),
    LEFT_JOIN("LEFT JOIN"),
    RIGHT_JOIN("RIGHT JOIN");

    private String name;

    JoinStrategy(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
