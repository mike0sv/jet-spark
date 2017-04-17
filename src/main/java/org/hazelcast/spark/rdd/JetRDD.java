package org.hazelcast.spark.rdd;

import com.hazelcast.jet.*;
import com.hazelcast.jet.stream.IStreamList;
import lombok.SneakyThrows;
import org.hazelcast.spark.JetSparkContext;
import org.hazelcast.spark.rdd.partitioning.Partitioning;
import org.hazelcast.spark.utils.EnhancedDAG;
import org.hazelcast.spark.utils.JetPeek;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.Processors.writeList;
import static com.hazelcast.jet.Traversers.traverseIterable;

@SuppressWarnings("unused")
public class JetRDD<T> extends AbstractJetRDD<JetRDD<T>> {
    public JetRDD(JetSparkContext context, Vertex vertex) {
        super(context, vertex);
        this.context = context;
    }

    JetRDD(JetRDD<T> other) {
        super(other);
    }

    public static <T> JetRDD<T> of(JetSparkContext context, ProcessorMetaSupplier metaSupplier) {
        Vertex vertex = new Vertex(context.getName("customRdd"), metaSupplier);
        return new JetRDD<T>(context, vertex);
    }

    public static <T> JetRDD<T> of(JetSparkContext context, ProcessorSupplier supplier) {
        Vertex vertex = new Vertex(context.getName("customRdd"), supplier);
        return new JetRDD<T>(context, vertex);
    }

    public static <T> JetRDD<T> of(JetSparkContext context, Distributed.Supplier<Processor> supplier) {
        Vertex vertex = new Vertex(context.getName("customRdd"), supplier);
        return new JetRDD<T>(context, vertex);
    }

    @Override
    protected void doUnpersist() {
        context.getJet().getList(vertex.getName() + "_cache").destroy();
    }

    @Override
    protected Vertex persistedVertex(DAG dag) {
        String name = vertex.getName() + "_cache";
        return dag.newVertex(name, writeList(name));
    }

    public <K, V> JetPairRDD<K, V> mapToPair(Distributed.Function<T, Map.Entry<K, V>> f) {
        Vertex vertex = new Vertex(context.getName("mapToPair"), Processors.map(f));
        JetPairRDD<K, V> rdd = new JetPairRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    @Deprecated
    public <K, V> JetPairRDDOld<K, V> mapToPair1(Distributed.Function<T, Map.Entry<K, V>> f) {
        Vertex vertex = new Vertex(context.getName("mapToPair"), Processors.map(f));
        JetPairRDDOld<K, V> rdd = new JetPairRDDOld<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public <T1> JetRDD<T1> map(Distributed.Function<T, T1> f) {
        Vertex vertex = new Vertex(context.getName("map"), Processors.map(f));
        JetRDD<T1> rdd = new JetRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public <T1> JetRDD<T1> flatMap(Distributed.Function<T, Iterable<T1>> f) {
        Vertex vertex = new Vertex(context.getName("flatMap"),
                Processors.flatMap(x -> traverseIterable(f.apply(((T) x)))));
        JetRDD<T1> rdd = new JetRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public <K, V> JetPairRDD<K, V> flatMapToPair(Distributed.Function<T, Iterable<Map.Entry<K, V>>> f) {
        Vertex vertex = new Vertex(context.getName("flatMapToPair"),
                Processors.flatMap(x -> traverseIterable(f.apply(((T) x)))));
        JetPairRDD<K, V> rdd = new JetPairRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }

    public JetRDD<T> filter(Distributed.Predicate<T> f) {
        Vertex vertex = new Vertex(context.getName("filter"), Processors.filter(f));
        JetRDD<T> rdd = new JetRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this));
        return rdd;
    }


    public void foreach(Distributed.Consumer<T> consumer) {
        Vertex vertex = new Vertex(context.getName("foreach"), JetPeek.foreach(consumer));
        JetRDD<T> rdd = new JetRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this, Partitioning.defaultPartitioning().allToOne()));
        rdd.collect();
    }

    public JetRDD<T> peek(Distributed.Consumer<T> consumer) {
        Vertex vertex = new Vertex(context.getName("peek"), JetPeek.peek(consumer));
        JetRDD<T> rdd = new JetRDD<>(context, vertex);
        rdd.parents.add(Parent.of(this, Partitioning.defaultPartitioning().allToOne()));
        return rdd;
    }
    @SneakyThrows
    public IStreamList<T> collectDistributed() {
        EnhancedDAG dag = new EnhancedDAG();
        buildDAG(dag);
        String name = context.getName("resultList");
        Vertex result = dag.newVertex(name, writeList(name));
        dag.edge(Edge.between(vertex, result));
        dag.execute(context.getJet());
        return context.getJet().getList(name);
    }

    @SneakyThrows
    public List<T> collect() {
        IStreamList<T> list = collectDistributed();
        LinkedList<T> ts = new LinkedList<>(list);
        list.destroy();
        return ts;
    }




}
