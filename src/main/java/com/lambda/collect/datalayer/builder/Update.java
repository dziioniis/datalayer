package com.lambda.collect.datalayer.builder;

import com.lambda.collect.datalayer.builder.types.Filters;
import com.lambda.collect.datalayer.builder.types.Updates;
import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.types.ColumnType;

public class Update<T extends ColumnType> extends QueryBuilder<T> {
    private Filters.Expr filters;
    private AbstractTable<?> tableClass;
    private Updates.UpdateExpr updates = null;

    private Update(AbstractTable<?> tableClass) {
        this.tableClass = tableClass;
    }

    public static <T extends ColumnType> Update<T> in(AbstractTable<?> tableClass) {
        return new Update<>(tableClass);
    }

    public Update<T> filteredBy(Filters.Expr filters) {
        this.filters = filters;
        return this;
    }

    public Update<T> update(Updates.UpdateExpr updates) {
        this.updates = updates;
        return this;
    }

    public String build() throws Exception {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("UPDATE ");
        stringBuilder.append(tableClass.name());

        if (updates == null) {
            throw new Exception("Update expression is empty");
        }

        stringBuilder.append("\nSET ").append(updates.toString());

        if (filters != null) {
            stringBuilder.append("\nWHERE ");
            stringBuilder.append(filters.toString());
        }

        return stringBuilder.toString().trim();
    }
}
