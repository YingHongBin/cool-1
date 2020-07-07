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
package com.nus.cool.core.iceberg.query;

import com.nus.cool.core.iceberg.aggregator.Aggregator;
import com.nus.cool.core.iceberg.aggregator.AggregatorFactory;
import com.nus.cool.core.iceberg.result.AggregatorResult;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.io.storevector.InputVector;
import com.nus.cool.core.schema.FieldType;
import com.sun.istack.internal.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
public class IcebergAggregation {

  private String timeRange;

  private Map<String, BitSet> group = new HashMap<>();

  private List<Map<String, BitSet>> groups = new ArrayList<>();

  private ChunkRS dataChunk;

  private MetaChunkRS metaChunk;

  private AggregatorFactory aggregatorFactory = new AggregatorFactory();

  /**
   * Init iceberg aggregation
   *
   * @param bs            bitset present target records
   * @param groupbyFields field name of group by fields
   * @param metaChunk     meta chunk of the cublet
   * @param dataChunk     data chunk
   * @param timeRange     time range
   */
  public void init(
      BitSet bs,
      @Nullable List<String> groupbyFields,
      MetaChunkRS metaChunk,
      ChunkRS dataChunk,
      String timeRange) {
    this.timeRange = timeRange;
    this.dataChunk = dataChunk;
    this.metaChunk = metaChunk;
    if (groupbyFields == null) {
      this.group.put("all", bs);
      return;
    }
    for (String groupbyField : groupbyFields) {
      MetaFieldRS metaField = metaChunk.getMetaField(groupbyField);
      FieldRS field = dataChunk.getField(groupbyField);
      switch (field.getFieldType()) {
        case UserKey:
        case ActionTime:
        case Action:
        case Segment:
          group(field, bs, metaField, GroupType.STRING);
          break;
        case Metric:
          group(field, bs, metaField, GroupType.NUMERIC);
          break;
        default:
          throw new UnsupportedOperationException("Unsupport field type: " + field.getFieldType());
      }
    }
    mergeGroups();
  }

  /**
   * Process iceberg aggregation
   *
   * @param aggregation aggregation defined in query
   * @return Aggregation results
   */
  public List<BaseResult> process(Aggregation aggregation) {
    String fieldName = aggregation.getFieldName();
    FieldType fieldType = this.metaChunk.getMetaField(fieldName).getFieldType();
    Map<String, AggregatorResult> resultMap = new HashMap<>();
    for (AggregatorFactory.AggregatorType aggregatorType : aggregation.getOperators()) {
      if (!checkOperatorIllegal(fieldType, aggregatorType)) {
        throw new IllegalArgumentException(fieldName + " can not process " + aggregatorType);
      }
      Aggregator aggregator = this.aggregatorFactory.create(aggregatorType);
      FieldRS field = this.dataChunk.getField(fieldName);
      aggregator.process(this.group, field, resultMap, this.metaChunk.getMetaField(fieldName));
    }
    List<BaseResult> results = new ArrayList<>();
    for (Map.Entry<String, AggregatorResult> entry : resultMap.entrySet()) {
      BaseResult result = new BaseResult();
      result.setTimeRange(this.timeRange);
      result.setFieldName(fieldName);
      result.setKey(entry.getKey());
      result.setAggregatorResult(entry.getValue());
      results.add(result);
    }
    return results;
  }

  /**
   * Get string value from compressed data
   *
   * @param metaField meta field
   * @param value     compressed data
   * @param type      field type
   * @return String value
   */
  private String getString(MetaFieldRS metaField, int value, GroupType type) {
    switch (type) {
      case STRING: {
        return metaField.getString(value);
      }
      case NUMERIC: {
        return String.valueOf(value);
      }
      default:
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Generate map store localId and bitset for the specific field
   *
   * @param field     group by field
   * @param bs        bitset for target records
   * @param metaField meta field of the group by field
   * @param type      field type
   */
  private void group(FieldRS field, BitSet bs, MetaFieldRS metaField, GroupType type) {
    Map<String, BitSet> group = new HashMap<>();
    if (field.isPreCal()) {
      InputVector key = field.getKeyVector();
      for (int i = 0; i < key.size(); i++) {
        //TODO NEED TEST
        String value = getString(metaField, i, type);
        BitSet bv = (BitSet) bs.clone();
        bv.and(field.getBitSets()[i]);
        if (bv.cardinality() == 0) {
          continue;
        }
        group.put(value, bv);
      }
    } else {
      Map<Integer, BitSet> id2Bs = new HashMap<>();
      InputVector value = field.getValueVector();
      for (int i = 0; i < value.size(); i++) {
        int nextPos = bs.nextSetBit(i);
        if (nextPos < 0) {
          break;
        }
        value.skipTo(nextPos);
        int id = value.next();
        if (id2Bs.get(id) == null) {
          BitSet groupBs = new BitSet(bs.size());
          groupBs.set(nextPos);
          id2Bs.put(id, groupBs);
        } else {
          BitSet groupBs = id2Bs.get(id);
          groupBs.set(nextPos);
        }
        i = nextPos;
      }
      for (Map.Entry<Integer, BitSet> entry : id2Bs.entrySet()) {
        group.put(getString(metaField, entry.getKey(), type), entry.getValue());
      }
    }
    this.groups.add(group);
  }

  /**
   * Check the aggregator is available for the target field type or not
   *
   * @param fieldType      target field type
   * @param aggregatorType aggregator type
   * @return available or not
   */
  private boolean checkOperatorIllegal(
      FieldType fieldType, AggregatorFactory.AggregatorType aggregatorType) {
    switch (aggregatorType) {
      case SUM:
      case AVERAGE:
      case MAX:
      case MIN: {
        if (!fieldType.equals(FieldType.Metric)) {
          return false;
        }
      }
      case COUNT:
        return true;
      case DISTINCT_COUNT:
        return !fieldType.equals(FieldType.Metric);
      default:
        throw new UnsupportedOperationException();
    }
  }

  /**
   * Merge individual group by field map as a map
   */
  private void mergeGroups() {
    this.group = this.groups.get(0);
    for (int i = 1; i < this.groups.size(); i++) {
      Map<String, BitSet> next = this.groups.get(i);
      Map<String, BitSet> merged = new HashMap<>();
      for (Map.Entry<String, BitSet> entry : this.group.entrySet()) {
        for (Map.Entry<String, BitSet> nextEntry : next.entrySet()) {
          String groupName = entry.getKey() + "|" + nextEntry.getKey();
          BitSet bs = (BitSet) entry.getValue().clone();
          bs.and(nextEntry.getValue());
          if (bs.isEmpty()) {
            continue;
          }
          merged.put(groupName, bs);
        }
      }
      this.group = merged;
    }
  }


  public enum GroupType {
    /**
     * String field
     */
    STRING,

    /**
     * Numeric field
     */
    NUMERIC
  }
}
