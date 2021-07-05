package com.lambda.collect.datalayer.builder;

import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.types.ColumnType;
import com.lambda.collect.datalayer.utils.MigrationAggregator;

public class Alter extends QueryBuilder {

    public static class AlterBuilder {

    }

    public static AlterTable table(AbstractTable<?> tableClass) {
        MigrationAggregator migrationAggregator = new MigrationAggregator();
        migrationAggregator.appendAlterTable(tableClass.name());
        return new AlterTable(migrationAggregator);
    }

    public static class AlterTable extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterTable(MigrationAggregator builder) {
            this.builder = builder;
        }

        public AlterColumn alterColumn(String columnName) {
            this.builder.appendColumn(columnName);
            return new AlterColumn(builder);
        }

        public DropColumn dropColumn(String columnName) {
            this.builder.appendDropColumn(columnName);
            return new DropColumn(builder);
        }

        public AlterTableName renameTo(String newName) {
            this.builder.appendRenameTableTo(newName);
            return new AlterTableName(builder);
        }

        public AlterConstraint renameConstraint(String oldName, String newName) {
            this.builder.appendRenameConstraint(oldName, newName);
            return new AlterConstraint(builder);
        }

        public AlterAddColumn addColumn(String columnName, ColumnType type) {
            this.builder.appendAddColumn(columnName, type);
            return new AlterAddColumn(builder);
        }

        public AlterAddForeignKey addForeignKey(String columnName) {
            this.builder.appendForeignKey(columnName);
            return new AlterAddForeignKey(builder);
        }


        public String build() {
            return builder.buildQuery();
        }
    }

    public static class AlterTableName extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterTableName(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class AlterConstraint extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterConstraint(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class AlterAddColumn extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterAddColumn(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class AlterAddReference extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterAddReference(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class AlterAddForeignKey extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterAddForeignKey(MigrationAggregator builder) {
            this.builder = builder;
        }

        public AlterAddReference addReference(String tableName, String columnName) {
            this.builder.appendReference(tableName, columnName);
            return new AlterAddReference(builder);
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class AlterColumn extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterColumn(MigrationAggregator builder) {
            this.builder = builder;
        }

        public AlterColumnName rename(String from, String to) {
            this.builder.appendRenameColumn(from, to);
            return new AlterColumnName(builder);
        }

        public AlterColumnType changeType(ColumnType type) {
            this.builder.appendColumnType(type);
            return new AlterColumnType(builder);
        }

        public DropNotNull dropNotNull() {
            this.builder.appendDropNotNull();
            return new DropNotNull(builder);
        }

        public AlterDefaultValue setDefaultValue(Object defaultValue) {
            this.builder.appendDefaultValue(defaultValue);
            return new AlterDefaultValue(builder);
        }

        public AlterDefaultValue setSequence(String sequenceName) {
            this.builder.appendSequence(sequenceName);
            return new AlterDefaultValue(builder);
        }

        public String build() {
            return builder.buildQuery();
        }

    }

    public static class AlterDefaultValue {
        private final MigrationAggregator builder;

        public AlterDefaultValue(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class DropNotNull extends AlterBuilder {
        private final MigrationAggregator builder;

        public DropNotNull(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class DropColumn extends AlterBuilder {
        private final MigrationAggregator builder;

        public DropColumn(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }


    public static class AlterColumnName extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterColumnName(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class AlterColumnType extends AlterBuilder {
        private final MigrationAggregator builder;

        public AlterColumnType(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }


}
