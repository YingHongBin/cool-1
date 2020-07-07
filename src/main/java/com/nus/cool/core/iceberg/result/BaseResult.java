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
package com.nus.cool.core.iceberg.result;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Data;

/**
 * @author hongbin
 * @version 0.1
 * @since 0.1
 */
@Data
public class BaseResult {

  private String timeRange;

  private String key;

  private String fieldName;

  private AggregatorResult aggregatorResult;

  public static List<BaseResult> merge(List<BaseResult> results) {
    BitSet bs = new BitSet();
    bs.set(0, results.size());
    for (int i = 0; i < bs.size(); i++) {
      i = bs.nextSetBit(i);
      if (i < 0) {
        break;
      }
      BaseResult res1 = results.get(i);
      for (int j = i + 1; j < bs.size(); j++) {
        j = bs.nextSetBit(j);
        if (j < 0) {
          break;
        }
        BaseResult res2 = results.get(j);
        if (res1.equals(res2)) {
          res1.getAggregatorResult().merge(res2.getAggregatorResult());
          bs.clear(j);
        }
      }
    }
    List<BaseResult> finalRes = new ArrayList<>();
    for (int i = 0; i < bs.size(); i++) {
      i = bs.nextSetBit(i);
      if (i < 0) {
        break;
      }
      finalRes.add(results.get(i));
    }
    return finalRes;
  }

  private boolean equalsKey(BaseResult another) {
    Set<String> set1 = new HashSet<>(Arrays.asList(this.getKey().split("|")));
    Set<String> set2 = new HashSet<>(Arrays.asList(another.getKey().split("|")));
    return set1.equals(set2);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if ((o instanceof BaseResult)) {
      BaseResult another = (BaseResult) o;
      if (this.equalsKey(another)) {
        return this.getTimeRange().equals(another.getTimeRange())
            && this.getKey().equals(another.getKey());
      } else {
        return false;
      }
    } else {
      return false;
    }
  }
}
