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

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
public class SelectionQuery {

  private SelectionType type;
  private String dimension;
  private List<String> values;
  private List<SelectionQuery> fields = new ArrayList<>();

  public SelectionType getType() {
    return type;
  }

  public void setType(SelectionType type) {
    this.type = type;
  }

  public String getDimension() {
    return dimension;
  }

  public void setDimension(String dimension) {
    this.dimension = dimension;
  }

  public List<String> getValues() {
    return values;
  }

  public void setValues(List<String> values) {
    this.values = values;
  }

  public List<SelectionQuery> getFields() {
    return fields;
  }

  public void setFields(List<SelectionQuery> fields) {
    this.fields = fields;
  }

  public enum SelectionType {
    and,
    or,
    filter
  }
}
