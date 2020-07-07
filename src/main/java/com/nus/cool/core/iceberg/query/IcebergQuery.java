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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.Getter;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
@Getter
public class IcebergQuery {

  /**
   * data source name
   */
  private String dataSource;

  /**
   * Selection query
   */
  private SelectionQuery selection;

  /**
   * Grouping fields
   */
  private List<String> groupFields;

  /**
   * Aggregations
   */
  private List<Aggregation> aggregations;

  /**
   * Time-range condition
   */
  private String timeRange;

  /**
   * Time granularity
   */
  private granularityType granularity;

  public static IcebergQuery read(InputStream in) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(in, IcebergQuery.class);
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return null;
  }

  public enum granularityType {
    DAY,

    MONTH,

    YEAR,

    NULL
  }
}
