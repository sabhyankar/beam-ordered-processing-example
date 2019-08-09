/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.transforms;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Ordering;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for {@link ProcessInOrder}.
 */
@RunWith(JUnit4.class)
public class ProcessInOrderTest {

  private static final int TEST_COUNT = 50;
  private static final int STATIC_KEY = 1;
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testListOrder() {

    List<Integer> inputList = generateRandomIntegers(TEST_COUNT);

    PCollection<Iterable<KV<Integer, Integer>>> actual =
        pipeline
            // 1. Create a pcollection of 50 random integers between 0 and 100
            .apply(Create.of(inputList))

            // 2. Convert these into a form expected by the ProcessInOrder transform
            // We use a static throw away key and use the value itself as the secondary key.
            // so input (5, 3) => (KV(1, KV(5,5), KV(1, KV(3,3)))
            .apply(MapElements.into(
                TypeDescriptors.kvs(TypeDescriptors.integers(),
                    TypeDescriptors.kvs(TypeDescriptors.integers(),
                        TypeDescriptors.integers())))
                .via(e -> KV.of(STATIC_KEY, KV.of(e, e))))

            // 3. Apply the transform we want to test.
            .apply(new ProcessInOrder<>())

            // 4. We only want to test if the Iterable<KV<Integer, Integer>> is sorted.
            // so we throw away the key.
            .apply(Values.create());

    PAssert.thatSingleton(actual).satisfies(
        e -> {

          // Create a sorted List<KV<Integer, Integer>> that
          // we expect the PTransform to return.
          List<KV<Integer, Integer>> expected =
              Ordering.natural()
                  .immutableSortedCopy(inputList).stream()
                  .map(v -> KV.of(v, v))
                  .collect(Collectors.toList());

          assertThat(e, equalTo(expected));
          return null;
        }
    );

    pipeline.run();
  }

  /**
   * Generate a {@link List} of random {@link Integer}s between 0 and 100.
   * @param count number of random integers to generate
   * @return List of random integers
   */
  private List<Integer> generateRandomIntegers(int count) {
    List<Integer> integers = new ArrayList<>();
    ThreadLocalRandom localRandom = ThreadLocalRandom.current();
    for (int i = 0; i < count; i++) {
      integers.add(localRandom.nextInt(0, 100 + 1));
    }

    return integers;
  }

}
