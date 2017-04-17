package org.hazelcast.spark.rdd;

import com.hazelcast.jet.*;
import com.hazelcast.jet.stream.IStreamMap;
import lombok.SneakyThrows;
import org.hazelcast.spark.JetSparkContext;
import org.hazelcast.spark.rdd.partitioning.Partitioning;
import org.hazelcast.spark.utils.EnhancedDAG;
import org.hazelcast.spark.utils.JetJoining;
import org.hazelcast.spark.utils.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static com.hazelcast.jet.KeyExtractors.entryKey;
import static com.hazelcast.jet.Processors.writeMap;
import static com.hazelcast.jet.Util.entry;


public class JetPairRDD<K, V> extends JetRDD<Map.Entry<K, V>> {
    public JetPairRDD(JetSparkContext context, Vertex vertex) {
        super(context, vertex);
    }

    private JetPairRDD(JetRDD<Map.Entry<K, V>> other) {
        super(other);
    }


    private static <K, V> JetPairRDD<K, V> of(JetRDD<Map.Entry<K, V>> rdd) {
        return new JetPairRDD<>(rdd);
    }

    @Override
    public JetPairRDD<K, V> filter(Distributed.Predicate<Map.Entry<K, V>> f) {
        return of(super.filter(f));
    }


    @Override
    public JetPairRDD<K, V> persist() {
        return of(super.persist());
    }

    public <V1> JetPairRDD<K, V1> mapValues(Distributed.Function<V, V1> f) {
        Vertex vertex = new Vertex(context.getName("mapValues"), Processors.map((e) -> {
            Map.Entry<K, V> e1 = (Map.Entry<K, V>) e;
            return entry(e1.getKey(), f.apply(e1.getValue()));
        }));
        JetPairRDD<K, V1> rdd = new JetPairRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public <V1> JetPairRDD<K, Pair<V, V1>> join(JetPairRDD<K, V1> other) {
        Vertex vertex = new Vertex(context.getName("join"), JetJoining.innerJoin());
        JetPairRDD<K, Pair<V, V1>> rdd = new JetPairRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this, Partitioning.by(entryKey()).distributed()));
        rdd.parents.add(Parent.of(other, Partitioning.by(entryKey()).distributed()));
        return rdd;
    }

    @SneakyThrows
    public IStreamMap<K, V> collectAsMapDistributed() {
        EnhancedDAG dag = new EnhancedDAG();
        buildDAG(dag);
        String name = context.getName("resultMap");
        Vertex result = dag.newVertex(name, writeMap(name));
        dag.edge(Edge.between(vertex, result));
        dag.execute(context.getJet());
        return context.getJet().getMap(name);
    }

    @SneakyThrows
    public Map<K, V> collectAsMap() {
        IStreamMap<K, V> map = collectAsMapDistributed();
        HashMap<K, V> rs = new HashMap<>(map);
        map.destroy();
        return rs;
    }

    public JetPairRDD<K, Iterable<V>> groupByKey() {
        Vertex vertex = new Vertex(context.getName("groupByKey"),
                Processors.<Map.Entry<K, V>, K, LinkedList<V>, Map.Entry<K, Iterable<V>>>groupAndAccumulate(
                        entryKey(),
                        (Distributed.Supplier<LinkedList<V>>) LinkedList<V>::new,
                        (acc, item) -> {
                            acc.add(item.getValue());
                            return acc;
                        }, Util::entry));
        JetPairRDD<K, Iterable<V>> rdd = new JetPairRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this, Partitioning.by(entryKey()).distributed()));
        return rdd;
    }
}
