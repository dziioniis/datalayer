package com.lambda.collect.datalayer.builder.types;


import com.lambda.collect.datalayer.builder.Insert;
import com.lambda.collect.datalayer.types.Column;
import com.lambda.collect.datalayer.types.ColumnType;
import com.lambda.collect.datalayer.types.PgInteger;

import java.util.Arrays;
import java.util.List;

import static com.lambda.collect.datalayer.utils.Collections.trim;

public class Updates {
    public static <T extends ColumnType> UpdateExpr set(Column<T> column, Object value) {
        return new SetObject(column.getColumnName(), value);
    }

    public static UpdateExpr increment(Column<PgInteger> column, int value, String tableName) {
        return new IncrementObject(column.getColumnName(), value, tableName);
    }

    public static <T extends ColumnType> UpdateExpr combine(List<UpdateExpr> updates) {
        return new Combine(updates);
    }

    public static <T extends ColumnType> UpdateExpr combine(UpdateExpr... updates) {
        return new Combine(Arrays.asList(updates));
    }

    public static class UpdateExpr {
    }

    public static class Combine extends UpdateExpr {
        public final List<UpdateExpr> setters;

        public Combine(List<UpdateExpr> setters) {
            this.setters = setters;
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            for (UpdateExpr expr : setters) {
                stringBuilder.append(expr.toString());
                stringBuilder.append(", ");
            }
            return trim(stringBuilder.toString().trim(), ',');
        }
    }

    public static class SetObject extends UpdateExpr {
        public final String column;
        public Object value;

        public SetObject(String column, Object value) {
            this.column = column;
            this.value = value;
        }

        @Override
        public String toString() {
            if (Insert.shouldEcranate(value)) {
                value = value instanceof String ? ((String) value).replaceAll("'", "''") : value;
                return "\"" + column + "\" = '" + value + "'";
            } else {
                return "\"" + column + "\" = " + value;
            }
        }
    }

    public static class IncrementObject extends UpdateExpr {
        public final String column;
        public final int value;
        public final String tableName;

        public IncrementObject(String column, int value, String tableName) {
            this.column = column;
            this.value = value;
            this.tableName = tableName;
        }

        @Override
        public String toString() {
            return column + " = " + tableName + "." + column + " + " + value;
        }
    }
}
