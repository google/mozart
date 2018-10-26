/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cse.mozart;

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.values.PCollection;

public interface MozartLogic extends Serializable{

  /**
   * Apply business logic to keyword elements.
   *
   * This method receives a PCollection with Keyword elements and must return a PCollection with
   * the updated Keyword elements (i.e.: with their values after applying the business logic.
   *
   * @param elements PCollection with Keyword elements. Each element is a Map<String, String>,
   *   where the key is the name of the column (as it appears on SA360) and the value is the
   *   value for that column.
   * @return PCollection with Keyword elements updated after applying the business logic. Any
   *   keywords omitted from this PCollection will not be uploaded to SA360 and thus will remain
   *   unchanged.
   */
  PCollection<Map<String, String>> applyLogic(PCollection<Map<String, String>> elements);

}
