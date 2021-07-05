package com.lambda.collect.datalayer.builder;

import com.lambda.collect.datalayer.builder.types.Updates;
import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.types.ColumnType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.lambda.collect.datalayer.utils.SelectAggregator.generateSelectFor;

public class Insert<T extends ColumnType> extends QueryBuilder<T> {
    private AbstractTable<?> tableClass;
    private List<Map<String, Object>> fields;
    private Updates.UpdateExpr updates = null;

    private Insert(AbstractTable<?> tableClass) {
        this.tableClass = tableClass;
    }

    public static <T extends ColumnType> Insert<T> into(AbstractTable<?> tableClass) {
        return new Insert<>(tableClass);
    }



    public Insert<T> values(Map<String, Object> values) {
        this.fields = Arrays.asList(values);
        return this;
    }

    public Insert<T> values(List<Map<String, Object>> values) {
        this.fields = values;
        return this;
    }

    public Insert<T> onConflict(Updates.UpdateExpr updates) {
        this.updates = updates;
        return this;
    }

    public String build() throws IllegalAccessException {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("INSERT INTO ");
        stringBuilder.append(tableClass.name()).append(" (");

        for (int i = 0; i < fields.get(0).keySet().size(); i++) {
            Object key = fields.get(0).keySet().toArray()[i];
            stringBuilder.append("\"").append(key).append("\"");
            if (i < fields.get(0).keySet().size() - 1) {
                stringBuilder.append(", ");
            }
        }
        stringBuilder.append(") ");

        StringBuilder valuesBuilder = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            Map<String, Object> field = fields.get(i);
            valuesBuilder.append("(");
            for (int j = 0; j < field.values().size(); j++) {
                Object value = field.values().toArray()[j];
                if (shouldEcranate(value)) {
                    value =  value instanceof String ? ((String) value).replaceAll("'", "''") :  value;

                    valuesBuilder.append("'").append(value).append("'");
                } else {
                    valuesBuilder.append(value);
                }
                if (j < field.values().size() - 1) {
                    valuesBuilder.append(", ");
                }
            }
            if (i < fields.size() - 1) {
                valuesBuilder.append("),\n");
            } else {
                valuesBuilder.append(")\n");
            }
        }

        stringBuilder.append("VALUES\n").append(valuesBuilder.toString().trim()).append("\n");
        if (updates != null) {
            stringBuilder.append("ON CONFLICT ON CONSTRAINT \"")
                    .append(tableClass.name())
                    .append("_").append(tableClass.getPrimaryKeys().get(0).getColumnName()).append("\"")
                    .append("\n");
            stringBuilder.append("DO UPDATE ");
            stringBuilder.append("\nSET ").append(updates.toString()).append("\n");
        }
        stringBuilder.append("RETURNING ");
        stringBuilder.append(generateSelectFor(tableClass));

        return stringBuilder.toString();
    }

    public static final Class<?>[] shouldEcranatedClasses = new Class<?>[]{
            String.class,
            Timestamp.class
    };

    public static boolean shouldEcranate(Object value) {
        for (Class<?> aClass : shouldEcranatedClasses) {
            if (aClass.isInstance(value)) {
                return true;
            }
        }
        return false;
    }
}
