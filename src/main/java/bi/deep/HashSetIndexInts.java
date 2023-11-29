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

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class HashSetIndexInts implements IndexedInts {
    private final HashMap<Object, Integer> objectToIndexMap;
    private final int size;

    public HashSetIndexInts(HashSet<Object> hashSet) {
        this.objectToIndexMap = new HashMap<>();
        int index = 0;
        for (Object object : hashSet) {
            objectToIndexMap.put(object, index++);
        }
        this.size = objectToIndexMap.size();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int get(int index) {
        Iterator<Integer> iterator = objectToIndexMap.values().iterator();
        for (int i = 0; i < index; i++) {
            iterator.next();
        }
        return iterator.next();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector) {
        inspector.visit("hashSet", objectToIndexMap);
    }
}
