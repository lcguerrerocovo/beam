/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.beam.learning.katas.coretransforms.cogroupbykey;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> fruits =
        pipeline.apply("Fruits",
            Create.of("apple", "banana", "cherry")
        );

    PCollection<String> countries =
        pipeline.apply("Countries",
            Create.of("australia", "brazil", "canada")
        );

    PCollection<String> output = applyTransform(fruits, countries);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<String> applyTransform(
      PCollection<String> fruits, PCollection<String> countries) {

    MapElements<String, KV<String, String>> mapper = MapElements.into(kvs(strings(), strings()))
        .via((String word) -> KV.of(word.substring(0, 1), word));

    PCollection<KV<String, String>> fruitskvs = fruits.apply("fruits mapper",mapper);
    PCollection<KV<String, String>> countrykvs = countries.apply("countries mapper",mapper);

    final TupleTag<String> fruitsTag = new TupleTag<>();
    final TupleTag<String> countriesTag = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> coGroupedCollection =
        KeyedPCollectionTuple
            .of(fruitsTag, fruitskvs)
            .and(countriesTag, countrykvs)
            .apply(CoGroupByKey.create());

    return coGroupedCollection.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {

      @ProcessElement
      public void processElement(@Element KV<String, CoGbkResult> in,
                                 OutputReceiver<String> out) {

        String firstLetter = in.getKey();
        String fruit = in.getValue().getOnly(fruitsTag);
        String country = in.getValue().getOnly(countriesTag);

        out.output(new WordsAlphabet(firstLetter, fruit, country).toString());

      }
    }));
  }

}