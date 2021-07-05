package com.lambda.collect.datalayer.types;

public enum SortOrder {
    ASC("ASC"),
    DESC("DESC");

    private String name;

    SortOrder(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
