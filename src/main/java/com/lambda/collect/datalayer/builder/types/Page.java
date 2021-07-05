package com.lambda.collect.datalayer.builder.types;

public class Page {
    private Integer number;
    private Integer size;

    private Page(Integer number, Integer size) {
        this.number = number;
        this.size = size;
    }

    public static Page set(Integer number, Integer size) {
        return new Page(number, size);
    }

    public String toQuery() {
        number = (number == null) ? 1 : number;
        size = (size == null) ? 20 : size;

        return "OFFSET " + size * (number - 1) + " ROWS FETCH NEXT " + size + " ROWS ONLY";
    }
}
