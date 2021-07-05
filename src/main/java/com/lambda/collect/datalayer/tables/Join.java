package com.lambda.collect.datalayer.tables;

import com.lambda.collect.datalayer.types.Column;
import com.lambda.collect.datalayer.types.ColumnType;

public class Join<T extends ColumnType, K> {
    public Column<T> fromColumn;
    public Column<T> toColumn;
    public AbstractTable<K> joinTable;
    public JoinStrategy type;

    private Join(AbstractTable<K> joinTable, Column<T> fromColumn, Column<T> toColumn) {
        this.joinTable = joinTable;
        this.toColumn = toColumn;
        this.fromColumn = fromColumn;
    }

    public static class JoinWith<K> {
        public AbstractTable<K> joinTable;

        public JoinWith(AbstractTable<K> joinTable) {
            this.joinTable = joinTable;
        }

        public <T extends ColumnType> Join<T, K> columns(Column<T> fromColumn, Column<T> toColumn) {
            return new Join<>(joinTable, fromColumn, toColumn);
        }
    }

    public static <K, M extends AbstractTable<K>> JoinWith<K> with(M table) {
        return new JoinWith<>(table);
    }

    public Join<T, K> columns(Column<T> fromColumn, Column<T> toColumn) {
        this.fromColumn = fromColumn;
        this.toColumn = toColumn;
        return this;
    }

    public Join<T, K> joinStrategy(JoinStrategy type) {
        this.type = type;
        return this;
    }
}
