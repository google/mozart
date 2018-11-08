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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mozart implements Serializable {

  private static final String H_ROW_TYPE = "Row Type";
  private static final String H_ACTION = "Action";
  private static final String H_STATUS = "Status";
  private static final String V_KEYWORD = "keyword";
  private static final String V_EDIT = "edit";
  private static final String V_ACTIVE = "Active";
  private static final String H_ADVERTISER_ID = "Advertiser ID";
  private static final Logger LOG = LoggerFactory.getLogger(Mozart.class);

  public interface MozartOptions extends PipelineOptions {
    @Description("Path to the input keywords file")
    ValueProvider<String> getInputKeywordsFile();

    void setInputKeywordsFile(ValueProvider<String> value);

    @Description("Keyword column names")
    ValueProvider<String> getKeywordColumnNames();

    void setKeywordColumnNames(ValueProvider<String> value);

    @Description("Output keywords file path")
    ValueProvider<String> getOutputKeywordsFile();

    void setOutputKeywordsFile(ValueProvider<String> value);
    
    @Description("Advertiser ID. This is used to filter the output so that it can be uploaded to "
        + "sFTP")
    ValueProvider<String> getAdvertiserId();

    void setAdvertiserId(ValueProvider<String> value);

  }

  /**
   * Get keywords PCollection.
   * 
   * This method returns a PCollection with all the keywords. The PCollection contains elements of
   * type {@code Map<String, String>}. Each element represents one keyword. The key is the column
   * name, and the value is the value for the column. Column names are the same as those in the
   * SA360 UI.
   * 
   * For example, for a given element (a keyword), the keyword text is element.get("Keyword"), and
   * the Max CPC is element.get("Keyword max CPC").
   * 
   * @param options Configuration options.
   * @param pipeline Beam pipeline that you want to use for this processing.
   * @return PCollection with keywords
   */
  public static PCollection<Map<String, String>> getKeywords(MozartOptions options,
      Pipeline pipeline) {

    options.getKeywordColumnNames().isAccessible();
    options.getAdvertiserId().isAccessible();

    return pipeline.apply("MozartReadKeywords", TextIO.read().from(options.getInputKeywordsFile()))
        // Create dictionary
        .apply("MozartCreateKWDict", ParDo.of(new DoFn<String, Map<String, String>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            String[] element = c.element().split(",");
            String[] headers = c.getPipelineOptions().as(MozartOptions.class)
                .getKeywordColumnNames().get().split(",");
            Map<String, String> newOutput = new HashMap<>();
            if (element.length == headers.length) {
              for (int i = 0; i < headers.length; i++) {
                newOutput.put(headers[i], element[i]);
              }
              newOutput.put(H_ROW_TYPE, V_KEYWORD);
              newOutput.put(H_ACTION, V_EDIT);
              newOutput.put(H_STATUS, V_ACTIVE);
              c.output(newOutput);
            } else {
              LOG.warn("Different length for headers and element. header: {}. element: {}", headers,
                  element);
            }
          }
        }));
  }

  /**
   * Write keywords to output.
   * 
   * This method writes keywords to GCS, so that the other Mozart elements (Composer) can do the
   * sFTP upload to SA360.
   * 
   * You should invoke this method when you have processed the keyword's PCollection according to
   * your business logic and are ready to push the new values to SA360.
   * 
   * @param options Configuration options.
   * @param keywordsAfterLogic PCollection with the keywords.
   */
  public static void writeKeywordsOutput(MozartOptions options,
      PCollection<Map<String, String>> keywordsAfterLogic) {
    keywordsAfterLogic
        .apply("MozartFlattenLines", ParDo.of(new DoFn<Map<String, String>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            MozartOptions options = c.getPipelineOptions().as(MozartOptions.class);
            String[] headers = options.getKeywordColumnNames().get().split(",");
            final List<String> values = new ArrayList<String>(headers.length);
            final Map<String, String> element = c.element();
            if(element.get(H_ADVERTISER_ID).equals(options.getAdvertiserId().get())) {
              List<String> outputHeaders = new ArrayList<>();
              outputHeaders.add(H_ROW_TYPE);
              outputHeaders.add(H_ACTION);
              outputHeaders.add(H_STATUS);
              outputHeaders.addAll(Arrays.asList(headers));
              outputHeaders.forEach(header -> values.add(element.get(header)));
              String valuesString = String.join(",", values);
              c.output(valuesString);
            }
          }
        })).apply("MozartWriteKeywords",
            TextIO.write().to(options.getOutputKeywordsFile()).withoutSharding());
  }

}
