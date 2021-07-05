package com.lambda.collect.datalayer.builder;

import com.lambda.collect.datalayer.utils.SequenceAggregator;

public class Sequence {

    public static class SequenceBuilder extends QueryBuilder {
        private final SequenceAggregator sequenceAggregator;

        public SequenceBuilder(SequenceAggregator sequenceAggregator) {
            this.sequenceAggregator = sequenceAggregator;
        }

        public SequenceBuilder increment(long value) {
            this.sequenceAggregator.appendIncrement(value);
            return this;
        }

        public SequenceBuilder minValue(long value) {
            this.sequenceAggregator.appendMinValue(value);
            return this;
        }

        public SequenceBuilder maxValue(long value) {
            this.sequenceAggregator.appendMaxValue(value);
            return this;
        }

        public SequenceBuilder startWith(long value) {
            this.sequenceAggregator.appendStartWith(value);
            return this;
        }

        public SequenceBuilder ownedBy(String tableName, String columnName) {
            this.sequenceAggregator.appendOwnedBy(tableName, columnName);
            return this;
        }

        public String build() {
            return this.sequenceAggregator.buildQuery();
        }
    }


    public static SequenceBuilder create(String name) {
        SequenceAggregator sequenceAggregator = new SequenceAggregator();
        sequenceAggregator.appendName(name);
        return new SequenceBuilder(sequenceAggregator);
    }



}
