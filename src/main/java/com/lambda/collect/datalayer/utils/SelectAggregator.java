package com.lambda.collect.datalayer.utils;

import com.lambda.collect.datalayer.builder.types.Filters;
import com.lambda.collect.datalayer.builder.types.Order;
import com.lambda.collect.datalayer.builder.types.Page;
import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.tables.Join;
import com.lambda.collect.datalayer.tables.JoinStrategy;
import com.lambda.collect.datalayer.types.Column;
import com.lambda.collect.datalayer.types.ColumnType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.lambda.collect.datalayer.utils.Collections.trim;

public class SelectAggregator {
    private final StringBuilder from = new StringBuilder();
    private final StringBuilder fields = new StringBuilder();
    private final StringBuilder filter = new StringBuilder();
    private final StringBuilder join = new StringBuilder();
    private final StringBuilder order = new StringBuilder();
    private final StringBuilder page = new StringBuilder();
    private final StringBuilder select = new StringBuilder("SELECT");

    public SelectAggregator appendSelect(String selectExpression) {
        this.select.append(selectExpression);
        return this;
    }

    public SelectAggregator appendFrom(AbstractTable<?> table) throws IllegalAccessException {
        this.from.append(" FROM ");
        this.from.append(table.name());
        return this;
    }

    public SelectAggregator appendFields(AbstractTable<?> table) throws IllegalAccessException {
        this.fields.append(generateSelectFor(table));
        return this;
    }

    public SelectAggregator filters(Filters.Expr filters) {
        this.filter.append("WHERE ");
        this.filter.append(filters.toString());
        return this;
    }

    public <T extends ColumnType> SelectAggregator order(Order<T> order) {
        this.order.append("ORDER BY ");
        for (int i = 0; i < order.getColumns().size(); i++) {
            Column<T> col = order.getColumns().get(i);
            this.order.append(col.getColumnName());
            if (i != order.getColumns().size() - 1) {
                this.order.append(",");
            }
        }
        this.order.append(" ").append(order.getOrder().getName());
        return this;
    }

    public <T extends ColumnType> SelectAggregator page(Page page) {
        this.page.append(page.toQuery());
        return this;
    }

    public <T extends ColumnType> SelectAggregator join(List<Join<T, ?>> joins) throws IllegalAccessException {
        for (Join<T, ?> joinProp : joins) {
            AbstractTable<?> tableFrom = joinProp.fromColumn.getParent();
            AbstractTable<?> tableTo = joinProp.toColumn.getParent();
            JoinStrategy type = joinProp.type;

            String generateSelectFor = generateSelectFor(tableFrom);
            fields.append(", ");
            fields.append(generateSelectFor);

            join.append("\n");
            join.append(type.getName()).append(" ").append(tableFrom.name()).append("\n");
            join
                    .append("ON ")
                    .append(tableFrom.name())
                    .append(".")
                    .append(joinProp.fromColumn.getColumnName())
                    .append(" = ")
                    .append(tableTo.name())
                    .append(".")
                    .append(joinProp.toColumn.getColumnName());
        }
        return this;
    }


    public String buildQuery() {
        return select
                .append(fields)
                .append("\n").append(from)
                .append("\n").append(join)
                .append("\n").append(filter)
                .append("\n").append(order)
                .append("\n").append(page)
                .toString();
    }

    public static String generateSelectFor(AbstractTable<?> table, List<Column<?>> columns) throws IllegalAccessException {
        StringBuilder stringBuilder = new StringBuilder();
        if (columns.isEmpty()) {
            List<Field> fields = Arrays
                    .stream(table.getClass().getDeclaredFields())
                    .filter(it -> it.getType() == Column.class)
                    .collect(Collectors.toList());

            for (Field field : fields) {
                Column<?> column = (Column<?>) field.get(table);

                stringBuilder
                        .append(" ")
                        .append(table.name())
                        .append(".")
                        .append("\"")
                        .append(column.getColumnName())
                        .append("\"")
                        .append(" AS ")
                        .append("\"")
                        .append(table.name().replace(".", "_"))
                        .append("_")
                        .append(column.getColumnName())
                        .append("\"")
                        .append(",");
            }
        }
        return trim(stringBuilder.toString(), ',');
    }

    public static String generateSelectFor(AbstractTable<?> table) throws IllegalAccessException {
        return generateSelectFor(table, new ArrayList<>());
    }
}
