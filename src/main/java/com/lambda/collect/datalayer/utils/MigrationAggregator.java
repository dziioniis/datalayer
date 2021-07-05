package com.lambda.collect.datalayer.utils;

import com.lambda.collect.datalayer.types.ColumnType;

import static com.lambda.collect.datalayer.builder.Insert.shouldEcranate;

public class MigrationAggregator {
    private final StringBuilder renameColumn = new StringBuilder();
    private final StringBuilder rename = new StringBuilder();
    private final StringBuilder type = new StringBuilder();
    private final StringBuilder alterTable = new StringBuilder();
    private final StringBuilder alterColumn = new StringBuilder();
    private final StringBuilder addColumn = new StringBuilder();
    private final StringBuilder defaultValue = new StringBuilder();
    private final StringBuilder addForeignKey = new StringBuilder();
    private final StringBuilder addReference = new StringBuilder();


    public void appendAlterTable(String tableName) {
        this.alterTable.append("ALTER TABLE ").append("\"").append(tableName).append("\"").append("\n");
    }

    public void appendDropTable(String tableName) {
        this.alterTable.append("DROP TABLE ").append("\"").append(tableName).append("\"").append(";\n");
    }

    public void appendColumn(String columnName) {
        this.alterColumn.append("ALTER COLUMN ").append("\"").append(columnName).append("\"").append("\n");
    }

    public void appendColumnType(ColumnType type) {
        this.type.append("TYPE ").append(type.getDbName()).append(";\n");
    }

    public void appendRenameTableTo(String tableName) {
        this.rename.append("RENAME TO ").append("\"").append(tableName).append("\"").append(";\n");
    }

    public void appendRenameColumn(String oldName, String newName) {
        this.renameColumn.append("RENAME COLUMN ").append("\"").append(oldName).append("\"").append(" TO ").append("\"").append(newName).append("\";\n");
    }

    public void appendDropColumn(String columnName) {
        this.alterColumn.append("DROP COLUMN IF EXISTS \"").append(columnName).append("\";\n");
    }

    public void appendDropNotNull() {
        this.alterColumn.append("DROP NOT NULL;\n");
    }

    public void appendAddColumn(String columnName, ColumnType type) {
        this.addColumn.append("ADD COLUMN \"").append(columnName).append("\"").append(" ").append(type.getDbName()).append(";");
    }

    public void appendDefaultValue(Object defaultValue) {
        if (shouldEcranate(defaultValue)) {
            this.defaultValue.append("SET DEFAULT '").append(defaultValue).append("';\n");
        } else {
            this.defaultValue.append("SET DEFAULT ").append(defaultValue).append(";\n");
        }
    }

    public void appendSequence(String sequenceName) {
        this.defaultValue.append("SET DEFAULT nextval('").append(sequenceName).append("');\n");
    }

    public void appendReference(String tableName, String columnName) {
        this.addReference.append(" REFERENCES \"").append(tableName).append("\"").append("(\"").append(columnName).append("\");");
    }

    public void appendForeignKey(String columnName) {
        this.addForeignKey.append(" ADD FOREIGN KEY(\"").append(columnName).append("\")");
    }

    public void appendRenameConstraint(String oldName, String newName) {
        this.rename.append("RENAME CONSTRAINT \"").append(oldName).append("\" TO \"").append(newName).append("\";\n");
    }

    public String buildQuery() {
        return alterTable
                .append(rename)
                .append(addColumn)
                .append(alterColumn)
                .append(defaultValue)
                .append(type)
                .append(renameColumn)
                .append(addForeignKey)
                .append(addReference)
                .toString();
    }


}
