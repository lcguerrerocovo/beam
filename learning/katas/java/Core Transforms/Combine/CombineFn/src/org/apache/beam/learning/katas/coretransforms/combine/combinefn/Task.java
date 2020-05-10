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

package org.apache.beam.learning.katas.coretransforms.combine.combinefn;

import java.io.Serializable;
import java.util.Objects;
import java.util.stream.StreamSupport;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers = pipeline.apply(Create.of(10, 20, 50, 70, 90));

    PCollection<Double> output = applyTransform(numbers);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<Double> applyTransform(PCollection<Integer> input) {
    return input.apply(Combine.globally(new AverageFn()));
  }


  public static class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {

    public static class Accum implements Serializable {
      int sum = 0;
      int count = 0;

      public Accum merge(Accum accum) {
        count += accum.count;
        sum += accum.sum;
        return this;
      }
    }

    @Override
    public Accum createAccumulator() {
      return new Accum();
    }

    @Override
    public Accum addInput(Accum accum, Integer input) {
      ++accum.count;
      accum.sum += input;
      return accum;
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accums) {
      return StreamSupport.stream(accums.spliterator(), false)
          .reduce(
              createAccumulator(),
              (Accum acc1, Accum acc2) -> acc2.merge(acc1)
          );
    }

    @Override
    public Double extractOutput(Accum accum) {
      return accum.sum/(new Double(accum.count));
    }


  }

}