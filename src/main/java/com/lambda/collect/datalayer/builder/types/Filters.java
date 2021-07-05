package com.lambda.collect.datalayer.builder.types;

import com.lambda.collect.datalayer.types.Column;
import com.lambda.collect.datalayer.types.ColumnType;

import java.util.List;

import static com.lambda.collect.datalayer.builder.Insert.shouldEcranate;


// Implement OR, AND tree dependency
public class Filters {
    public static Expr or(Expr left, Expr right) {
        return new OrFilter(left, right);
    }

    public static Expr and(Expr left, Expr right) {
        return new AndFilter(left, right);
    }

    public static <T extends ColumnType> Expr eq(Column<T> left, Object value) {
        return new EqFilter(new Var<>(left), new Const(value));
    }

    public static <T extends ColumnType> Expr eq(Column<T> left, String jsonBKey, Object value) {
        return new EqjFilter(new Var<>(left), new Const(value), jsonBKey);
    }

    public static <T extends ColumnType> Expr greaterEq(Column<T> left, Object value) {
        return new GreatEqFilter(new Var<>(left), new Const(value));
    }

    public static <T extends ColumnType> Expr lessEq(Column<T> left, Object value) {
        return new LessEqFilter(new Var<>(left), new Const(value));
    }

    public static <T extends ColumnType> Expr greater(Column<T> left, Object value) {
        return new GreatFilter(new Var<>(left), new Const(value));
    }

    public static <T extends ColumnType> Expr less(Column<T> left, Object value) {
        return new LessFilter(new Var<>(left), new Const(value));
    }

    public static <T extends ColumnType> Expr in(Column<T> left, List<?> values) {
        return new InFilter(new Var<>(left), new ConstList(values));
    }

    public static <T extends ColumnType> Expr in(Column<T> left, String jsonBKey, List<?> values) {
        return new InJsonbFilter(new Var<>(left), jsonBKey, new ConstList(values));
    }

    public static <T extends ColumnType> Expr like(Column<T> left, Object value) {
        return new LikeFilter(new Var<>(left), new Const(value));
    }

    public static <T extends ColumnType> Expr ilike(Column<T> left, Object value) {
        return new ILikeFilter(new Var<>(left), new Const(value));
    }

    public static <T extends ColumnType> Expr between(Column<T> left, Object fromValue, Object toValue) {
        return new BetweenFilter(new Var<>(left), new AndBetween(new Const(fromValue), new Const(toValue)));
    }

    public static <T extends ColumnType> Expr isNotNull(Column<T> left) {
        return new IsNotNullFilter(new Var<>(left));
    }

    public static <T extends ColumnType> Expr isNull(Column<T> left) {
        return new IsNullFilter(new Var<>(left));
    }

    public static class Expr {
    }

    public static class Var<T extends ColumnType> extends Expr {
        public final Column<T> column;

        public Var(Column<T> column) {
            this.column = column;
        }

        @Override
        public String toString() {
            return column.getParent().name() + "." + "\"" + column.getColumnName() + "\"";
        }
    }

    public static class Const extends Expr {
        public final Object val;

        public Const(Object val) {
            this.val = val;
        }

        @Override
        public String toString() {
            if (shouldEcranate(val)) {
                return "'" + val.toString() + "'";
            } else {
                return val.toString();
            }
        }
    }

    public static class ConstList extends Expr {
        public final List<?> values;

        public ConstList(List<?> values) {
            this.values = values;
        }

        @Override
        public String toString() {
            StringBuilder listVal = new StringBuilder();
            for (int i = 0; i < values.size(); i++) {
                if (values.get(i) instanceof String ||
                        values.get(i) instanceof Integer) {
                    listVal.append("'").append(values.get(i).toString()).append("'");
                } else {
                    listVal.append(values.get(i).toString());
                }
                if (i < values.size() - 1) {
                    listVal.append(",");
                }
            }
            return listVal.toString();
        }
    }

    public static class AndBetween extends Expr {
        public final Const from;
        public final Const to;

        public AndBetween(Const from, Const to) {
            this.from = from;
            this.to = to;
        }

        @Override
        public String toString() {
            return from + " AND " + to;
        }
    }

    public static class InFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public InFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left.toString() + " IN (" + right.toString() + ")";
        }
    }

    public static class InJsonbFilter extends Expr {
        public final Expr left;
        public final Expr right;
        public final String jsonBKey;

        public InJsonbFilter(Expr left, String jsonBKey, Expr right) {
            this.left = left;
            this.right = right;
            this.jsonBKey = jsonBKey;
        }

        @Override
        public String toString() {
            return left.toString() + "->>'" + jsonBKey + "' IN (" + right.toString() + ")";
        }
    }

    public static class BetweenFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public BetweenFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left + " BETWEEN " + right;
        }
    }

    public static class LikeFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public LikeFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left.toString() + " LIKE " + right.toString();
        }
    }

    public static class ILikeFilter extends LikeFilter {
        public ILikeFilter(Expr left, Expr right) {
            super(left, right);
        }

        @Override
        public String toString() {
            return left.toString() + " ILIKE " + right.toString();
        }
    }

    public static class GreatEqFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public GreatEqFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left.toString() + " >= " + right.toString();
        }
    }

    public static class LessEqFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public LessEqFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left.toString() + " <= " + right.toString();
        }
    }

    public static class LessFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public LessFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left.toString() + " < " + right.toString();
        }
    }

    public static class GreatFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public GreatFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left.toString() + " > " + right.toString();
        }
    }

    public static class IsNotNullFilter extends Expr {
        public final Expr left;

        public IsNotNullFilter(Expr left) {
            this.left = left;
        }

        @Override
        public String toString() {
            return left.toString() + " IS NOT NULL";
        }
    }

    public static class IsNullFilter extends Expr {
        public final Expr left;

        public IsNullFilter(Expr left) {
            this.left = left;
        }

        @Override
        public String toString() {
            return left.toString() + " IS NULL";
        }
    }

    public static class EqFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public EqFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return left.toString() + " = " + right.toString();
        }
    }

    public static class EqjFilter extends Expr {
        public final Expr left;
        public final Expr right;
        public final String jKey;

        public EqjFilter(Expr left, Expr right, String jKey) {
            this.left = left;
            this.right = right;
            this.jKey = jKey;
        }

        @Override
        public String toString() {
            return left.toString() + "->>'" + jKey + "' = " + compileStringFromObject(right);
        }

        private static String compileStringFromObject(Expr right) {
            StringBuilder stringBuilder = new StringBuilder();
            try {
                int intExpr = Integer.parseInt(right.toString());
                stringBuilder.append("'")
                        .append(intExpr)
                        .append("'");
            } catch (NumberFormatException ex) {
                stringBuilder.append(right);
            }
            return stringBuilder.toString();
        }
    }

    public static class OrFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public OrFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return "(" + left.toString() + " or " + right.toString() + ")";
        }
    }

    public static class AndFilter extends Expr {
        public final Expr left;
        public final Expr right;

        public AndFilter(Expr left, Expr right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public String toString() {
            return "(" + left.toString() + " and " + right.toString() + ")";
        }
    }
}
