/*
 * Copyright 2020 Cool Squad Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nus.cool.core.iceberg.aggregator;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
public class AggregatorFactory {

  public Aggregator create(AggregatorType type) {
    switch (type) {
      case MAX:
        return new MaxAggregator();
      case MIN:
        return new MinAggregator();
      case SUM:
        return new SumAggregator();
      case COUNT:
        return new CountAggregator();
      case AVERAGE:
        return new AverageAggregator();
      case DISTINCT_COUNT:
        return new CountDistinctAggregator();
      default:
        throw new IllegalArgumentException("Unsupported aggregator type: " + type);
    }
  }

  public enum AggregatorType {

    /**
     * Count aggregator
     */
    COUNT,

    /**
     * Sum aggregator, only used in numeric field
     */
    SUM,

    /**
     * Average aggregator, only used in numeric field
     */
    AVERAGE,

    /**
     * Max aggregator, only used in numeric field
     */
    MAX,

    /**
     * Min aggregator, only used in numeric field
     */
    MIN,

    /**
     * Distinct count aggregator, only used in string field
     */
    DISTINCT_COUNT
  }
}
