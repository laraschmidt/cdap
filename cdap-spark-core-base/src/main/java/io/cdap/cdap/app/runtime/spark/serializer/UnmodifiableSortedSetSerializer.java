/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.app.runtime.spark.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Kryo serializer for Collections.UnmodifiableSortedSet
 */
public class UnmodifiableSortedSetSerializer extends Serializer<SortedSet> {
  @Override
  public void write(Kryo kryo, Output output, SortedSet sortedSet) {
    CollectionSerializer collectionSerializer = new CollectionSerializer();
    collectionSerializer.write(kryo, output, sortedSet);
  }

  @Override
  public SortedSet read(Kryo kryo, Input input, Class<SortedSet> aClass) {
    CollectionSerializer collectionSerializer = new CollectionSerializer();
    Class<?> sortedSetClass = TreeSet.class;
    Object object = collectionSerializer.read(kryo, input, (Class<Collection>) sortedSetClass);

    return Collections.unmodifiableSortedSet((SortedSet) object);
  }
}
