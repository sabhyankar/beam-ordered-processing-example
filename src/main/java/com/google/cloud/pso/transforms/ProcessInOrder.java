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

import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.ExternalSorter.Options.SorterType;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * The {@link ProcessInOrder} class is a {@link PTransform} that takes in a
 * {@link PCollection} of {@link KV} with a primary key (T) and a value which is another
 * {@link KV} with a secondary key (K) and a value (V).
 *
 * This transform will group by the primary key and sort by the secondary key (K) all the values (V).
 *
 * @param <T> primary key to group by
 * @param <K> secondary key which will be used to sort the values V
 * @param <V> values that need to be sorted
 */
public class ProcessInOrder<T, K, V> extends
    PTransform<PCollection<KV<T, KV<K, V>>>, PCollection<KV<T,Iterable<KV<K, V>>>>> {

  @Override
  public PCollection<KV<T, Iterable<KV<K, V>>>> expand(PCollection<KV<T, KV<K, V>>> input) {

    return
    input
        // 1. Group by T so we have an iterable of KV<K, V> that can be sorted.
        .apply("GroupByKey",
            GroupByKey.create())

        // 2. SortValues will sort the KV<K, V> by the secondary key.
        .apply("SortValues",
            SortValues.create(
                BufferedExternalSorter.options().withExternalSorterType(SorterType.NATIVE)
            ));

  }
}
