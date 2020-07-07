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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
@Getter
public class SelectionQuery {

  /**
   * Selection type to distinguish AND/OR/FILTER
   */
  private SelectionType type;

  /**
   * Filter field
   */
  private String dimension;

  /**
   * Filter values
   */
  private List<String> values;

  /**
   * Selection query for AND/OR clause
   */
  private List<SelectionQuery> fields = new ArrayList<>();

  public enum SelectionType {
    AND,
    OR,
    FILTER
  }
}
