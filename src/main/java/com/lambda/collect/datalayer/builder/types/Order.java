package com.lambda.collect.datalayer.builder.types;


import com.lambda.collect.datalayer.types.Column;
import com.lambda.collect.datalayer.types.ColumnType;
import com.lambda.collect.datalayer.types.SortOrder;

import java.util.List;

public class Order<T extends ColumnType> {
    private final List<Column<T>> columns;
    private final SortOrder order;

    public Order(List<Column<T>> columns, SortOrder order) {
        this.columns = columns;
        this.order = order;
    }

    public List<Column<T>> getColumns() {
        return columns;
    }

    public SortOrder getOrder() {
        return order;
    }
}
