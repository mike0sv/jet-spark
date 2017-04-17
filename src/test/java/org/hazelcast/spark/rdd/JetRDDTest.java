package org.hazelcast.spark.rdd;

import com.hazelcast.jet.Distributed;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.*;

/**
 * Created by mike on 16-Apr-17.
 */
public class JetRDDTest extends AbstractJetRDDTest {
    @Test
    public void flatMapToPair() throws Exception {
        Distributed.Function<String, Iterable<Map.Entry<String, Character>>> f = c -> range(0, 1).mapToObj(i -> entry(c, c.charAt(i))).collect(toList());
        List<Map.Entry<String, Character>> collect = cityRDD.flatMapToPair(f).collect();
        assertTrue(collect.equals(cityList.stream().flatMap(
                x -> StreamSupport.stream(f.apply(x).spliterator(), false)).collect(toList())));
    }



    @Test
    public void flatMap() throws Exception {
        Distributed.Function<String, Iterable<Character>> f = c -> Arrays.asList(c.charAt(0), c.charAt(1));
        List<Character> collect = cityRDD.flatMap(f).collect();
        assertTrue(collect.equals(cityList.stream().flatMap(
                x -> StreamSupport.stream(f.apply(x).spliterator(), false)).collect(toList())));
    }


    @Test
    public void doUnpersist() throws Exception {

    }

    @Test
    public void persistedVertex() throws Exception {

    }

    @Test
    public void mapToPair() throws Exception {
        Map<String, String> collect = cityRDD.mapToPair1(c -> entry(c, countriesMap.get(c)))
                .collect();
        assertTrue(collect.equals(countriesMap));
    }

    @Test
    public void map() throws Exception {
        List<Character> collect = cityRDD.map(c -> c.charAt(0)).collect();

        assertTrue(new HashSet<>(collect)
                .equals(
                        cityList.stream().map(c -> c.charAt(0)).collect(Collectors.toSet())));
    }

    @Test
    public void filter() throws Exception {
        List<String> collect = cityRDD.filter(c -> capitalsMap.containsValue(c)).collect();
        assertTrue(new HashSet<>(collect).equals(new HashSet<>(capitals)));
    }

    @Test
    public void collect() throws Exception {
        assertTrue(setEquals(cityList, cityRDD.collect()));
    }


}