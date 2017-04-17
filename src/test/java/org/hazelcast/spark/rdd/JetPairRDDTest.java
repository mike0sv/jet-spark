package org.hazelcast.spark.rdd;

import com.hazelcast.jet.Distributed;
import org.hazelcast.spark.utils.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.Util.entry;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.*;


public class JetPairRDDTest extends AbstractJetRDDTest{


    @Test
    public void mapToPair() throws Exception {
        Distributed.Function<Map.Entry<String, String>, Map.Entry<String, Integer>> f =
                e -> entry(e.getKey() + "_", e.getValue().length());
        Map<String, Integer> collect = countriesPairRDD.mapToPair(f).collectAsMap();
        assertTrue(collect.equals(transform(countriesMap, f)));
    }

    @Test
    public void map() throws Exception {
        Distributed.Function<Map.Entry<String, String>, String> f = e -> e.getKey() + ", " + e.getValue();
        List<String> collect = countriesPairRDD.map(f).collect();
        assertTrue(setEquals(collect, countriesMap.entrySet().stream().map(f).collect(Collectors.toList())));
    }

    @Test
    public void mapValues() throws Exception {
        Map<String, Integer> collect = countriesPairRDD.mapValues(String::length).collectAsMap();

        assertTrue(collect.equals(transformValues(countriesMap, String::length)));
    }

    @Test
    public void join() throws Exception {
        JetPairRDD<String, Integer> other = countriesPairRDD.mapValues(String::length);
        JetPairRDD<String, Pair<String, Integer>> join = countriesPairRDD.join(other);
        Map<String, Pair<String, Integer>> collect = join.collectAsMap();
        assertTrue(collect.equals(transformValues(countriesMap, v -> new Pair<>(v, v.length()))));
    }

    @Test
    public void filter() throws Exception {
        Distributed.Predicate<Map.Entry<String, String>> f = e -> capitalsMap.containsValue(e.getKey());
        JetPairRDD<String, String> filter = countriesPairRDD.filter(f);
        Map<String, String> collect = filter.collectAsMap();
        assertTrue(collect.equals(
                countriesMap.entrySet().stream().filter(f).collect(toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    @Test
    public void groupByKey() throws Exception {
        LinkedList<String> test = new LinkedList<>();
        test.addAll(cityList);
        test.addAll(cityList);
        JetPairRDD<String, Long> groupBy = context.parallelize(test)
                .mapToPair(c -> entry(c, c))
                .groupByKey()
                .mapValues(i -> StreamSupport.stream(i.spliterator(), false).count());
        groupBy.foreach(System.out::println);
        Map<String, Long> collect = groupBy.collectAsMap();
        assertTrue(setEquals(collect.keySet(), cityList));
        assertTrue(collect.values().stream().filter(x -> x != 2).count() == 0);
    }

    @Test
    public void collect() throws Exception {

    }

    private <K, V, K1, V1> Map<K1, V1> transform(Map<K, V> map, Function<Map.Entry<K, V>, Map.Entry<K1, V1>> f) {
        return map.entrySet().stream().map(f).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private <K, V, V1> Map<K, V1> transformValues(Map<K, V> map, Function<V, V1> f) {
        return map.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> f.apply(e.getValue())));
    }

}