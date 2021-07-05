package com.lambda.collect.datalayer.builder;


import com.lambda.collect.datalayer.builder.types.Filters;
import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.types.ColumnType;

public class Delete<T extends ColumnType> extends QueryBuilder<T> {
    private Filters.Expr filters;
    private AbstractTable<?> tableClass;

    private Delete(AbstractTable<?> tableClass) {
        this.tableClass = tableClass;
    }

    public static <T extends ColumnType> Delete<T> from(AbstractTable<?> tableClass) {
        return new Delete<>(tableClass);
    }

    public Delete<T> filteredBy(Filters.Expr filters) {
        this.filters = filters;
        return this;
    }

    public String build() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DELETE ");

        stringBuilder.append("FROM ");
        stringBuilder.append(tableClass.name()).append(" ");

        if (filters != null) {
            stringBuilder.append("WHERE ");
            stringBuilder.append(filters.toString());
        }

        return stringBuilder.toString().trim();
    }
}
