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
package com.google.cse.mozart.examples;

import com.google.cse.mozart.Mozart;
import com.google.cse.mozart.Mozart.MozartOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mozart example pipeline.
 *
 * <p>This is an example of a Beam pipeline that uses Mozart
 */
public class GCSInput {

  // Extend MozartOptions to add your custom options to the pipeline
  interface MozartExampleOptions extends MozartOptions {
    @Description("Path to the custom input file (customer data table)")
    ValueProvider<String> getInputCustomDataFile();

    void setInputCustomDataFile(ValueProvider<String> value);

    @Description("Custom data column names")
    ValueProvider<String> getCustomDataColumnNames();

    void setCustomDataColumnNames(ValueProvider<String> value);
  }

  private static final Logger LOG = LoggerFactory.getLogger(GCSInput.class);

  public static void main(String[] args) {

    // Build your Pipeline as usual
    PipelineOptionsFactory.register(MozartOptions.class);
    MozartExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(MozartExampleOptions.class);
    options.getCustomDataColumnNames().isAccessible();
    Pipeline p = Pipeline.create(options);

    // Load additional custom (non-SA360) data. For example: a file containing information about
    // whether a certain brand is in promotion state
    PCollection<Map<String, String>> customData =
        p.apply("ReadCustomData", TextIO.read().from(options.getInputCustomDataFile()))
            // Create dictionary
            .apply(
                "CreateCustomDataDict",
                ParDo.of(
                    new DoFn<String, Map<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String[] element = c.element().split(",");
                        String[] headers =
                            c.getPipelineOptions()
                                .as(MozartExampleOptions.class)
                                .getCustomDataColumnNames()
                                .get()
                                .split(",");
                        Map<String, String> newOutput = new HashMap<>();
                        if (element.length == headers.length) {
                          for (int i = 0; i < headers.length; i++) {
                            newOutput.put(headers[i], element[i]);
                          }
                          c.output(newOutput);
                        } else {
                          LOG.warn(
                              "Different length for headers and element. header: {}. element: {}",
                              headers,
                              element);
                        }
                      }
                    }));

    // Create a view of your custom data (this is necessary to pass it as side input to your
    // main PTransform
    PCollectionView<List<Map<String, String>>> customDataView =
        customData.apply("CreateCustomDataView", View.asList());

    final String maxCPCPromo = "5";
    final String maxCPCNormal = "1";

    // Define your PTransform. Here is where your business logic is implemented
    PTransform<? super PCollection<Map<String, String>>, PCollection<Map<String, String>>>
        businessTransform =
            ParDo.of(
                    new DoFn<Map<String, String>, Map<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        List<Map<String, String>> customData = c.sideInput(customDataView);
                        final Map<String, String> element = c.element();
                        // Skip if element is empty
                        if (element.size() > 0) {
                          String newMaxCPC = maxCPCNormal;
                          for (Map<String, String> customEntry : customData) {
                            if (element.get("Keyword").contains(customEntry.get("brand"))) {
                              if (customEntry.get("status").equals("promotion")) {
                                newMaxCPC = maxCPCPromo;
                              }
                            }
                          }
                          element.put("Keyword max CPC", newMaxCPC);
                          c.output(element);
                        }
                      }
                    })
                .withSideInputs(customDataView);

    // Use Mozart to complete the pipeline
    // First, get the PCollection with all the keywords
    PCollection<Map<String, String>> keywordsBeforeLogic = Mozart.getKeywords(options, p);

    // Then, apply your business logic over the keywords
    PCollection<Map<String, String>> keywordsAfterLogic =
        keywordsBeforeLogic.apply(businessTransform);

    // Lastly, use Mozart to write the keywords output
    Mozart.writeKeywordsOutput(options, keywordsAfterLogic);

    // Run your pipeline as usual
    p.run();
  }
}
