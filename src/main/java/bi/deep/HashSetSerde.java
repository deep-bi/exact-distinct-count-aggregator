/*
 *    Copyright 2023 Deep BI, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package bi.deep;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashSet;

public class HashSetSerde extends ComplexMetricSerde {

    public static final String TYPE_NAME = "hashSet";

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public ComplexMetricExtractor<HashSet<Object>> getExtractor() {
        return new ComplexMetricExtractor() {
            @Override
            public Class<?> extractedClass() {
                return HashSet.class;
            }

            @Nullable
            @Override
            public Object extractValue(InputRow inputRow, String metricName) {
                return inputRow.getRaw(metricName);
            }
        };
    }

    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {
        try {
            int numBytes = buffer.remaining();
            byte[] bytes = new byte[numBytes];
            buffer.get(bytes);
            ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectStream = new ObjectInputStream(byteStream);
            HashSet deserializedHashSet = (HashSet) objectStream.readObject();
            builder.setType(ValueType.COMPLEX);
            GenericIndexed<Object> genericIndexed = GenericIndexed.fromIterable(deserializedHashSet, getObjectStrategy());
            builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), genericIndexed));
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ObjectStrategy getObjectStrategy() {
        return new ObjectStrategy<HashSet>() {
            @Override
            public Class<HashSet> getClazz() {
                return HashSet.class;
            }

            @Override
            public HashSet<Object> fromByteBuffer(ByteBuffer buffer, int i) {
                byte[] bytes = new byte[i];
                buffer.get(bytes);
                ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
                try {
                    ObjectInputStream objectStream = new ObjectInputStream(byteStream);
                    return (HashSet<Object>) objectStream.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public byte[] toBytes(HashSet obj) {
                try {
                    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                    ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
                    objectStream.writeObject(obj);
                    objectStream.close();
                    return byteStream.toByteArray();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public int compare(HashSet o1, HashSet o2) {
                return o1.hashCode() - o2.hashCode();
            }
        };
    }
}
