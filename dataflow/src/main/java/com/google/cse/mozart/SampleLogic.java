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

import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class SampleLogic implements MozartLogic {

  @Override
  public PCollection<Map<String, String>> applyLogic(PCollection<Map<String, String>> elements) {

    return elements.apply(ParDo.of(new DoFn<Map<String, String>, Map<String, String>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        final Map<String, String> element = c.element();
        // Replace next line with your own logic
        element.put("Keyword max CPC", "1234");
        c.output(element);
      }
    }));

  }

}
