package com.hazelcast.prometheusdemo;

import com.hazelcast.map.MapStore;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.primitives.Ints.asList;
import static com.google.common.primitives.Ints.toArray;
import static java.util.stream.Collectors.toMap;

public class MapDBBackedMapStore implements MapStore<Integer, List<Integer>> {
    
    private final ConcurrentMap<Integer, int[]> primeFactors;
    {
        System.out.print("Initializing MapDB... ");
        DB db = DBMaker.fileDB(System.getProperty("java.io.tmpdir") + "/hello.mapdb").make();
        primeFactors = db.hashMap("primeFactors", Serializer.INTEGER, Serializer.INT_ARRAY).createOrOpen();
        System.out.println("done.");
    }

    @Override
    public void store(Integer key, List<Integer> value) {
        primeFactors.put(key, toArray(value));
    }

    @Override
    public void storeAll(Map<Integer, List<Integer>> map) {
        map.forEach(this::store);
    }

    @Override
    public void delete(Integer key) {
        primeFactors.remove(key);
    }

    @Override
    public void deleteAll(Collection<Integer> keys) {
        keys.forEach(this::delete);
    }

    @Override
    public List<Integer> load(Integer key) {
        if (!primeFactors.containsKey(key)) {
            return null;
        }
        return asList(primeFactors.get(key));
    }

    @Override
    public Map<Integer, List<Integer>> loadAll(Collection<Integer> keys) {
        return keys.stream()
                .filter(primeFactors::containsKey)
                .map(key -> Tuples.pair(key, asList(primeFactors.get(key))))
                .collect(toMap(Pair::getOne, Pair::getTwo));
    }

    @Override
    public Iterable<Integer> loadAllKeys() {
        return primeFactors.keySet();
    }
}
