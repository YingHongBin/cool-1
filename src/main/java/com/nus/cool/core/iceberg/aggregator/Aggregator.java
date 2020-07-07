package com.nus.cool.core.iceberg.aggregator;

import com.nus.cool.core.iceberg.result.AggregatorResult;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import java.util.BitSet;
import java.util.Map;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
public interface Aggregator {

  /**
   * Process aggregation for specific groups
   *
   * @param groups    A map to store group fields and corresponding bitset
   * @param field     Aggregation field
   * @param resultMap A map to store aggregation results
   * @param metaField Meta field of aggregation field
   */
  void process(Map<String, BitSet> groups, FieldRS field, Map<String, AggregatorResult> resultMap,
      MetaFieldRS metaField);
}
