package com.lambda.collect.datalayer.builder;

import com.lambda.collect.datalayer.builder.types.Filters;
import com.lambda.collect.datalayer.builder.types.Order;
import com.lambda.collect.datalayer.builder.types.Page;
import com.lambda.collect.datalayer.tables.AbstractTable;
import com.lambda.collect.datalayer.tables.Join;
import com.lambda.collect.datalayer.types.ColumnType;
import com.lambda.collect.datalayer.utils.SelectAggregator;

import java.util.List;

public class Select extends QueryBuilder {

    public static class SelectBuilder {

    }

    public static class SelectFrom extends SelectBuilder{
        private final SelectAggregator builder;

        public SelectFrom(SelectAggregator builder) {
            this.builder = builder;
        }

        public <T extends ColumnType> SelectJoined joined(List<Join<T, ?>> joins) throws IllegalAccessException {
            return new SelectJoined(builder).apply(joins);
        }

        public SelectFiltered filteredBy(Filters.Expr filters) {
            return new SelectFiltered(builder).apply(filters);
        }

        public <T extends ColumnType> SelectSorted ordered(Order<T> order) {
            return new SelectSorted(builder).apply(order);
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class SelectJoined extends SelectBuilder{
        private final SelectAggregator builder;

        public SelectJoined(SelectAggregator builder) {
            this.builder = builder;
        }

        public <T extends ColumnType> SelectJoined apply(List<Join<T, ?>> joins) throws IllegalAccessException {
            this.builder.join(joins);
            return this;
        }

        public SelectFiltered filteredBy(Filters.Expr filters) {
            return new SelectFiltered(builder).apply(filters);
        }

        public <T extends ColumnType> SelectSorted ordered(Order<T> order) {
            return new SelectSorted(builder).apply(order);
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class SelectSorted extends SelectBuilder{
        private final SelectAggregator builder;

        public SelectSorted(SelectAggregator builder) {
            this.builder = builder;
        }

        public <T extends ColumnType> SelectSorted apply(Order<T> order) {
            this.builder.order(order);
            return this;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class SelectPaged extends SelectBuilder {
        private final SelectAggregator builder;

        public SelectPaged(SelectAggregator builder) {
            this.builder = builder;
        }

        public <T extends ColumnType> SelectPaged apply(Page page) {
            this.builder.page(page);
            return this;
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static class SelectFiltered extends SelectBuilder{
        private final SelectAggregator builder;

        public SelectFiltered(SelectAggregator builder) {
            this.builder = builder;
        }

        public SelectFiltered apply(Filters.Expr filters) {
            this.builder.filters(filters);
            return this;
        }

        public SelectPaged paged(Page page) {
            return new SelectPaged(builder).apply(page);
        }

        public <T extends ColumnType> SelectSorted ordered(Order<T> order) {
            return new SelectSorted(builder).apply(order);
        }

        public String build() {
            return builder.buildQuery();
        }
    }

    public static SelectFrom from(AbstractTable<?> table) throws IllegalAccessException {
        SelectAggregator aggregator = new SelectAggregator();
        aggregator.appendFrom(table).appendFields(table);
        return new SelectFrom(aggregator);
    }
}
