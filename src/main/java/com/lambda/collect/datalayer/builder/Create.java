package com.lambda.collect.datalayer.builder;


import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.types.Column;

import java.lang.reflect.Field;

public class Create {
    private AbstractTable<?> tableClass;

    private Create(AbstractTable<?> tableClass) {
        this.tableClass = tableClass;
    }

    public static Create from(AbstractTable<?> tableClass) {
        return new Create(tableClass);
    }

    public String build() throws IllegalAccessException {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(tableClass.name()).append(" (");
        StringBuilder columnsBuilder = new StringBuilder();

        for (int i = 0; i < tableClass.getClass().getDeclaredFields().length; i++) {
            Field field = tableClass.getClass().getDeclaredFields()[i];
            if (field.getType() == Column.class) {
                Column<?> column = (Column<?>) field.get(tableClass);
                columnsBuilder
                        .append(column.getColumnName())
                        .append(" ")
                        .append(column.isAutoIncrement() ? "SERIAL" : column.getColumnType().getDbName())
                        .append(" ")
                        .append(column.getDefaultValue() != null ? "DEFAULT " + column.getDefaultValue() : "");
                columnsBuilder.append(column.getIsNullable() ? "" : " NOT NULL");
//                columnsBuilder.append(column.getPrimaryKeyName().isEmpty() ? "" : " CONSTRAINT " + column.getPrimaryKeyName() + " PRIMARY KEY");

                Column<?> referenced = column.getReferenced();
                columnsBuilder.append(referenced != null ? " REFERENCES " + referenced.getParent().name() + " (" + referenced.getColumnName() + ")" : "");

                if (i < tableClass.getClass().getDeclaredFields().length - 1) {
                    columnsBuilder.append(",");
                }
            }
        }
        StringBuilder primaryKey = new StringBuilder();
        if (!tableClass.getPrimaryKeys().isEmpty()) {
            columnsBuilder.append(",");
            primaryKey.append("\nCONSTRAINT " + tableClass.name() + "_" + tableClass.getPrimaryKeys().get(0).getColumnName());
            primaryKey.append(" PRIMARY KEY(");

            for (int i = 0; i < tableClass.getPrimaryKeys().size(); i++) {
                Column<?> column = tableClass.getPrimaryKeys().get(i);
                primaryKey.append(column.getColumnName());

                if (i != tableClass.getPrimaryKeys().size() - 1) {
                    primaryKey.append(",");
                }
            }
            primaryKey.append(")");
        }


        StringBuilder uniqueKey = new StringBuilder();
        if (!tableClass.getUniqueKeys().isEmpty()) {
            String columnName = tableClass.getUniqueKeys().get(0).getColumnName();

            uniqueKey.append("\n,CONSTRAINT " + tableClass.name() + "_" + columnName);
            uniqueKey.append(" UNIQUE(");

            for (int i = 0; i < tableClass.getUniqueKeys().size(); i++) {
                Column<?> column = tableClass.getUniqueKeys().get(i);
                uniqueKey.append(column.getColumnName());

                if (i != tableClass.getUniqueKeys().size() - 1) {
                    uniqueKey.append(",");
                }
            }
            uniqueKey.append(")");
        }

        return sb.append(columnsBuilder).append(primaryKey).append(uniqueKey).append(");").toString();
    }
}
