package com.lambda.collect.datalayer.types;

import com.lambda.collect.datalayer.tables.AbstractTable;

import java.util.function.Supplier;

public class Column<T extends ColumnType> {
    private final String columnName;
    private boolean isNullable = false;
    private boolean autoIncrement = false;
    private Object defaultValue = null;
    private String primaryKeyName = "";
    private String uniqueKeyName = "";
    private final AbstractTable<?> parent;
    private Column<T> referenced = null;
    private final T columnType;

    private Column(AbstractTable<?> parent, String columnName, Supplier<T> columnType) {
        this.columnName = columnName;
        this.parent = parent;
        this.columnType = columnType.get();
    }

    public static <K> Column<PgInteger> integer(AbstractTable<K> parent, String columnName) {
        return new Column<>(parent, columnName, PgInteger::new);
    }

    public static Column<PgText> text(AbstractTable<?> parent, String columnName) {
        return new Column<>(parent, columnName, PgText::new);
    }

    public static Column<PgJsonb> jsonb(AbstractTable<?> parent, String columnName) {
        return new Column<>(parent, columnName, PgJsonb::new);
    }

    public static Column<PgTimestamp> timestamp(AbstractTable<?> parent, String columnName) {
        return new Column<>(parent, columnName, PgTimestamp::new);
    }

    public static Column<PgVarchar> varchar(AbstractTable<?> parent, String columnName,
                                            int stringLength) {
        return new Column<>(parent, columnName, () -> new PgVarchar(stringLength));
    }

    public static Column<PgBigDecimal> decimal(AbstractTable<?> parent, String columnName,
                                               int precision, int scale) {
        return new Column<>(parent, columnName, () -> new PgBigDecimal(precision, scale));
    }

    public static Column<PgBoolean> bool(AbstractTable<?> parent, String columnName) {
        return new Column<>(parent, columnName, PgBoolean::new);
    }

    public static Column<PgTime> time(AbstractTable<?> parent, String columnName) {
        return new Column<>(parent, columnName, PgTime::new);
    }

    public String getAlias() {
        return getParent().name().replace(".", "_") + "_" + columnName;
    }

    public Column<T> references(Column<T> column) {
        this.referenced = column;
        return this;
    }

    public AbstractTable<?> getParent() {
        return parent;
    }

    public Column<T> getReferenced() {
        return referenced;
    }

    public Column<T> defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public Column<T> isNullable() {
        this.isNullable = true;
        return this;
    }

    public Column<T> autoIncrement() {
        this.autoIncrement = true;
        return this;
    }

    public Column<T> primaryKey() {
        this.primaryKeyName = parent.name() + "_" + columnName;
        return this;
    }

    public Column<T> unique() {
        this.uniqueKeyName = columnName;
        return this;
    }

    public String getPrimaryKeyName() {
        return parent.name() + "_" + columnName;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public String getColumnName() {
        return columnName;
    }

    public T getColumnType() {
        return columnType;
    }

    public boolean getIsNullable() {
        return isNullable;
    }
}