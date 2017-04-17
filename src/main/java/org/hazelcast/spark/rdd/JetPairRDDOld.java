package org.hazelcast.spark.rdd;

import com.hazelcast.jet.*;
import com.hazelcast.jet.stream.IStreamMap;
import lombok.SneakyThrows;
import org.hazelcast.spark.JetSparkContext;
import org.hazelcast.spark.rdd.partitioning.Partitioning;
import org.hazelcast.spark.utils.EnhancedDAG;
import org.hazelcast.spark.utils.JetJoining;
import org.hazelcast.spark.utils.JetPeek;
import org.hazelcast.spark.utils.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static com.hazelcast.jet.KeyExtractors.entryKey;
import static com.hazelcast.jet.Processors.writeMap;
import static com.hazelcast.jet.Util.entry;


@SuppressWarnings("unused")
@Deprecated
public class JetPairRDDOld<K, V> extends AbstractJetRDD<JetPairRDDOld<K, V>> {
    public JetPairRDDOld(JetSparkContext context, Vertex vertex) {
        super(context, vertex);
    }

    @Override
    protected void doUnpersist() {
        context.getJet().getMap(vertex.getName() + "_cache").destroy();
    }

    @Override
    protected Vertex persistedVertex(DAG dag) {
        String name = vertex.getName() + "_cache";
        return dag.newVertex(name, writeMap(name));
    }

    public <K1, V1> JetPairRDDOld<K1, V1> mapToPair(Distributed.Function<Map.Entry<K, V>, Map.Entry<K1, V1>> f) {
        Vertex vertex = new Vertex(context.getName("mapToPair"), Processors.map(f));
        JetPairRDDOld<K1, V1> rdd = new JetPairRDDOld<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public <T1> JetRDD<T1> map(Distributed.Function<Map.Entry<K, V>, T1> f) {
        Vertex vertex = new Vertex(context.getName("map"), Processors.map(f));
        JetRDD<T1> rdd = new JetRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public <V1> JetPairRDDOld<K, V1> mapValues(Distributed.Function<V, V1> f) {
        Vertex vertex = new Vertex(context.getName("mapValues"), Processors.map((e) -> {
            Map.Entry<K, V> e1 = (Map.Entry<K, V>) e;
            return entry(e1.getKey(), f.apply(e1.getValue()));
        }));
        JetPairRDDOld<K, V1> rdd = new JetPairRDDOld<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public <V1> JetPairRDDOld<K, Pair<V, V1>> join(JetPairRDDOld<K, V1> other) {
        Vertex vertex = new Vertex(context.getName("join"), JetJoining.innerJoin());
        JetPairRDDOld<K, Pair<V, V1>> rdd = new JetPairRDDOld<>(context, vertex);
        rdd.parents.add(Parent.of(this, Partitioning.by(entryKey()).distributed()));
        rdd.parents.add(Parent.of(other, Partitioning.by(entryKey()).distributed()));
        return rdd;
    }

    public JetPairRDDOld<K, V> filter(Distributed.Predicate<Map.Entry<K, V>> f) {
        Vertex vertex = new Vertex(context.getName("filter"), Processors.filter(f));
        JetPairRDDOld<K, V> rdd = new JetPairRDDOld<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public JetPairRDDOld<K, Iterable<V>> groupByKey() {
        Vertex vertex = new Vertex(context.getName("groupByKey"),
                Processors.<Map.Entry<K, V>, K, LinkedList<V>, Map.Entry<K, Iterable<V>>>groupAndAccumulate(
                        entryKey(),
                        (Distributed.Supplier<LinkedList<V>>) LinkedList<V>::new,
                        (acc, item) -> {
                            acc.add(item.getValue());
                            return acc;
                        }, Util::entry));
        JetPairRDDOld<K, Iterable<V>> rdd = new JetPairRDDOld<>(context, vertex);
        rdd.parents.add(Parent.of(this, Partitioning.by(entryKey()).distributed()));
        return rdd;
    }

    public void foreach(Distributed.Consumer<Map.Entry<K, V>> consumer) {
        Vertex vertex = new Vertex(context.getName("foreach"), JetPeek.foreach(consumer));
        JetPairRDDOld<K, V> rdd = new JetPairRDDOld<>(context, vertex);
        rdd.parents.add(Parent.of(this, Partitioning.defaultPartitioning().allToOne()));
        rdd.collect();
    }

    public JetPairRDDOld<K, V> peek(Distributed.Consumer<Map.Entry<K, V>> consumer) {
        Vertex vertex = new Vertex(context.getName("peek"), JetPeek.peek(consumer));
        JetPairRDDOld<K, V> rdd = new JetPairRDDOld<>(context, vertex);
        rdd.parents.add(Parent.of(this, Partitioning.defaultPartitioning().allToOne()));
        return rdd;
    }

    @SneakyThrows
    public IStreamMap<K, V> collectDistributed() {
        EnhancedDAG dag = new EnhancedDAG();
        buildDAG(dag);
        String name = context.getName("resultMap");
        Vertex result = dag.newVertex(name, writeMap(name));
        dag.edge(Edge.between(vertex, result));
        dag.execute(context.getJet());
        return context.getJet().getMap(name);
    }

    @SneakyThrows
    public Map<K, V> collect() {
        IStreamMap<K, V> map = collectDistributed();
        HashMap<K, V> rs = new HashMap<>(map);
        map.destroy();
        return rs;
    }
}
