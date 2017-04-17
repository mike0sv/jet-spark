package org.hazelcast.spark.rdd;

import com.hazelcast.jet.Distributed;
import org.hazelcast.spark.utils.Pair;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.Util.entry;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.*;


public class JetPairRDDOldTest extends AbstractJetRDDTest {
    @Test
    public void doUnpersist() throws Exception {

    }

    @Test
    public void persistedVertex() throws Exception {

    }

    @Test
    public void mapToPair() throws Exception {
        Distributed.Function<Map.Entry<String, String>, Map.Entry<String, Integer>> f =
                e -> entry(e.getKey() + "_", e.getValue().length());
        Map<String, Integer> collect = countriesPairRDDOld.mapToPair(f).collect();
        assertTrue(collect.equals(transform(countriesMap, f)));
    }

    @Test
    public void map() throws Exception {
        Distributed.Function<Map.Entry<String, String>, String> f = e -> e.getKey() + ", " + e.getValue();
        List<String> collect = countriesPairRDDOld.map(f).collect();
        assertTrue(setEquals(collect, countriesMap.entrySet().stream().map(f).collect(Collectors.toList())));
    }

    @Test
    public void mapValues() throws Exception {
        Map<String, Integer> collect = countriesPairRDDOld.mapValues(String::length).collect();

        assertTrue(collect.equals(transformValues(countriesMap, String::length)));
    }

    @Test
    public void join() throws Exception {
//        countriesPairRDDOld.foreach();
        JetPairRDDOld<String, Integer> other = countriesPairRDDOld.mapValues(String::length);
//        other.foreach();
        JetPairRDDOld<String, Pair<String, Integer>> join = countriesPairRDDOld.join(other);
//        System.out.println(join.toDebugString());
        join.foreach(System.out::println);
        Map<String, Pair<String, Integer>> collect = join.collect();
        assertTrue(collect.equals(transformValues(countriesMap, v -> new Pair<>(v, v.length()))));
    }

    @Test
    public void filter() throws Exception {
        Distributed.Predicate<Map.Entry<String, String>> f = e -> capitalsMap.containsValue(e.getKey());
        Map<String, String> collect = countriesPairRDDOld.filter(f).collect();
        assertTrue(collect.equals(
                countriesMap.entrySet().stream().filter(f).collect(toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    @Test
    public void groupByKey() throws Exception {
        LinkedList<String> test = new LinkedList<>();
        test.addAll(cityList);
        test.addAll(cityList);
        Map<String, Long> collect = context.parallelize(test).mapToPair1(c -> entry(c, c)).groupByKey().mapValues(i ->
                StreamSupport.stream(i.spliterator(), false).count()).collect();

        assertTrue(setEquals(collect.keySet(), cityList));
        assertTrue(collect.values().stream().filter(x -> x != 2).count() == 0);
    }

    @Test
    public void collect() throws Exception {

    }

    <K, V, K1, V1> Map<K1, V1> transform(Map<K, V> map, Function<Map.Entry<K, V>, Map.Entry<K1, V1>> f) {
//        HashMap<K1, V1> answer = new HashMap<>();
//        map.entrySet().forEach(e -> {
//            Map.Entry<K1, V1> e1 = f.apply(e);
//            answer.put(e1.getKey(), e1.getValue());
//        });
        return map.entrySet().stream().map(f).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    <K, V, V1> Map<K, V1> transformValues(Map<K, V> map, Function<V, V1> f) {
//        HashMap<K, V1> answer = new HashMap<>();
//        map.entrySet().forEach(e -> {
//            answer.put(e.getKey(), f.apply(e.getValue()));
//        });
        return map.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> f.apply(e.getValue())));
    }


}