package com.lambda.collect.datalayer.builder;

import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.utils.MigrationAggregator;

public class Drop extends QueryBuilder{

    public static class DropBuilder {
    }

    public static DropTable table(AbstractTable<?> tableClass) {
        MigrationAggregator migrationAggregator = new MigrationAggregator();
        migrationAggregator.appendDropTable(tableClass.name());
        return new DropTable(migrationAggregator);
    }

    public static class DropTable extends DropBuilder {
        private final MigrationAggregator builder;

        public DropTable(MigrationAggregator builder) {
            this.builder = builder;
        }

        public String build() {
            return builder.buildQuery();
        }
    }
}
