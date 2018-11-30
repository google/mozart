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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.cse.mozart.Mozart;
import com.google.cse.mozart.Mozart.MozartOptions;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mozart example pipeline.
 *
 * <p>This is an example of a Beam pipeline that uses Mozart. This example pulls data from Firebase
 * realtime database.
 */
public class FirebaseInput {

  // Extend MozartOptions to add your custom options to the pipeline
  interface MozartExampleOptions extends MozartOptions {
    @Description("Path to the custom input file (customer data table)")
    ValueProvider<String> getInputCustomDataFile();

    void setInputCustomDataFile(ValueProvider<String> value);

    @Description("Custom data column names")
    ValueProvider<String> getCustomDataColumnNames();

    void setCustomDataColumnNames(ValueProvider<String> value);
  }

  private static final Logger LOG = LoggerFactory.getLogger(FirebaseInput.class);

  public static String getFirebaseData(String path) throws UnirestException, IOException {
    // Use the Google credential to generate an access token
    InputStream credentialsStream = FirebaseInput.class.getResourceAsStream("service_account.json");
    GoogleCredential credentials = GoogleCredential.fromStream(credentialsStream);
    credentialsStream.close();
    final GoogleCredential scopedCredentials =
        credentials.createScoped(
            Arrays.asList(
                "https://www.googleapis.com/auth/firebase.database",
                "https://www.googleapis.com/auth/userinfo.email"));
    scopedCredentials.refreshToken();
    String token = scopedCredentials.getAccessToken();
    HttpResponse<String> jsonResponse =
        Unirest.get("https://fir-test-a5923.firebaseio.com" + path)
            .queryString("access_token", token)
            .asString();
    return jsonResponse.getBody();
  }

  public static void main(String[] args) throws IOException {

    // Build your Pipeline as usual
    PipelineOptionsFactory.register(MozartOptions.class);
    MozartExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(MozartExampleOptions.class);
    options.getCustomDataColumnNames().isAccessible();
    options.getInputCustomDataFile().isAccessible();
    Pipeline p = Pipeline.create(options);

    // Use the Google credential to generate an access token

    // Define your PTransform. Here is where your business logic is implemented
    PTransform<? super PCollection<Map<String, String>>, PCollection<Map<String, String>>>
        businessTransform =
            ParDo.of(
                new DoFn<Map<String, String>, Map<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    final Map<String, String> element = c.element();
                    // Skip if element is empty
                    if (element.size() > 0) {
                      try {
                        String newMaxCPC = getFirebaseData("/maxCPC.json");
                        LOG.info("newMaxCPC from Firebase: {}", newMaxCPC);
                        element.put("Keyword max CPC", newMaxCPC);
                      } catch (Exception e) {
                        LOG.error("Error while processing element", e);
                      }
                      c.output(element);
                    }
                  }
                });

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
