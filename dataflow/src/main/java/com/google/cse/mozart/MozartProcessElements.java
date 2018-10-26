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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Mozart main pipeline file.
 *
 * <p>
 * This is the main Class for Mozart Pipeline. The Mozart pipeline defined in this file reads
 * Kewyords from GCS and creates the dictionary elements for the Kewyords (Map<String, String>). It
 * then applies the business logic (implemented as MozartLogic). As last steps, it writes output
 * back to GCS.
 */
public class MozartProcessElements {

  public interface MozartOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    ValueProvider<String> getInputFile();

    void setInputFile(ValueProvider<String> value);

    @Description("Output file path")
    ValueProvider<String> getOutputFile();

    void setOutputFile(ValueProvider<String> value);

    @Description("Header for output CSV file")
    ValueProvider<String> getHeader();

    void setHeader(ValueProvider<String> value);
  }

  public static void main(String[] args) {
    MozartOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(MozartOptions.class);
    Pipeline p = Pipeline.create(options);
    options.getHeader().isAccessible();
    // Replace with your own MozartLogic implementation
    MozartLogic logic = new SampleLogic();

    PCollection<Map<String, String>> elementsBeforeLogic =
        p.apply(TextIO.read().from(options.getInputFile()))
            // Create dictionary
            .apply(ParDo.of(new DoFn<String, Map<String, String>>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                String[] element = c.element().split(",");
                String[] headers =
                    c.getPipelineOptions().as(MozartOptions.class).getHeader().get().split(",");
                Map<String, String> newOutput = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                  newOutput.put(headers[i], element[i]);
                }
                c.output(newOutput);
              }
            }));
    // Processing
    PCollection<Map<String, String>> elementsAfterLogic = logic.applyLogic(elementsBeforeLogic);
    // From dictionary to string line
    elementsAfterLogic.apply(ParDo.of(new DoFn<Map<String, String>, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        String[] headers =
            c.getPipelineOptions().as(MozartOptions.class).getHeader().get().split(",");
        final List<String> values = new ArrayList<String>(headers.length);
        final Map<String, String> element = c.element();
        Arrays.asList(headers).forEach(header -> values.add(element.get(header)));
        String valuesString = String.join(",", values);
        c.output(valuesString);
      }
    })).apply(TextIO.write().to(options.getOutputFile()).withoutSharding());

    p.run();
  }
}
