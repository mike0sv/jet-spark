package org.hazelcast.spark.rdd;

import com.hazelcast.jet.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.hazelcast.spark.JetSparkContext;
import org.hazelcast.spark.rdd.partitioning.Partitioning;
import org.hazelcast.spark.utils.EnhancedDAG;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.jet.connector.hadoop.WriteHdfsP.writeHdfs;
import static java.util.stream.IntStream.range;


@SuppressWarnings("unused")
@AllArgsConstructor(access = AccessLevel.PACKAGE)
abstract class AbstractJetRDD<T extends AbstractJetRDD<T>> {

    static ILogger LOGGER = Logger.getLogger(AbstractJetRDD.class);
    JetSparkContext context;
    Vertex vertex;
    private Partitioning partitioning;
    LinkedList<Parent> parents = new LinkedList<>();
    private boolean persisting = false;
    private boolean isPersisted = false;

    AbstractJetRDD(AbstractJetRDD other) {
        this(other.context, other.vertex, other.partitioning, other.parents,  other.persisting, other.isPersisted);
    }
    AbstractJetRDD(JetSparkContext context, Vertex vertex) {
        this.context = context;
        this.vertex = vertex;
    }

    AbstractJetRDD(JetSparkContext context, Vertex vertex, Partitioning partitioning) {
        this.context = context;
        this.vertex = vertex;
        this.partitioning = partitioning;
    }

    @SuppressWarnings("unchecked")
    public T persist() {
        persisting = true;
        return (T) this;
    }

    public T cache() {
        return persist();
    }

    public void unpersist() {
        persisting = false;
        doUnpersist();
        isPersisted = false;
    }

    protected abstract void doUnpersist();

    AbstractJetRDD partitionBy(Partitioning partitioning) {
        this.partitioning = partitioning;
        return this;
    }

    public String toDedagString() {
        EnhancedDAG dag = buildDAG(new EnhancedDAG());
        dag.edge(Edge.between(vertex, dag.newVertex("out", () -> null)));
        return dag.toString();
    }

    public String toDebugString() {
        return toDebugString(0, partitioning);
    }

    private String toDebugString(int offset, Partitioning partitioning) {
        List<String> ps = parents.stream()
                .map(p -> p.rdd.toDebugString(offset + 1, p.edgePartitioner == null ? partitioning : p.edgePartitioner))
                .collect(Collectors.toList());
        StringBuilder builder = new StringBuilder();
        range(0, offset).forEach(x -> builder.append("  "));
        builder.append(vertex.getName());
        if (partitioning != null) {
            builder.append(" partitioned by ").append(partitioning.toString());
        }
        builder.append("\n");
        ps.forEach(p -> builder.append(p).append("\n"));
        return builder.toString();
    }

    EnhancedDAG buildDAG(EnhancedDAG dag) {
        if (isPersisted) {
            vertex = persistedVertex(dag);
        } else {
            int parallelism = vertex.getLocalParallelism();
            Vertex existing = dag.getVertex(vertex.getName());
            vertex = existing == null ? dag.newVertex(vertex.getName(), vertex.getSupplier()) : existing;
            if (parallelism > 0) {
                vertex.localParallelism(parallelism);
            }
            int i = 0;
            for (Parent parent : parents) {
                parent.rdd.buildDAG(dag);
                Edge edge = dag.createEdge(parent.rdd.vertex, vertex, i);

                if (parent.edgePartitioner != null && parent.rdd.partitioning != null) {
                    LOGGER.warning("Edge partitioning and parent partitioning are both set, discarding parent partitioning");
                }
                if (parent.edgePartitioner != null) {
                    Partitioning.apply(parent.edgePartitioner, edge);
                } else {
                    Partitioning.apply(parent.rdd.partitioning, edge);
                }
                dag.edge(edge);

                ++i;
            }
            if (persisting) {
                dag.edge(Edge.from(vertex, 1).to(persistedVertex(dag), 0));
                dag.addJobEndedCallback(() -> isPersisted = true);
            }
        }
        return dag;
    }

    protected abstract Vertex persistedVertex(DAG dag);

    @SneakyThrows
    public void saveAsText(String path) {
        JobConf jobConfig = new JobConf();
        jobConfig.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(jobConfig, new Path(path));
        EnhancedDAG dag = new EnhancedDAG();
        buildDAG(dag);
        Vertex out = dag.newVertex(context.getName("writeHdfs"), writeHdfs(jobConfig));
        dag.edge(Edge.between(vertex, out));
        dag.execute(context.getJet());
    }



    @AllArgsConstructor
    static class Parent {
        AbstractJetRDD rdd;
        Partitioning edgePartitioner;

        static Parent of(AbstractJetRDD rdd) {
            return new Parent(rdd, null);
        }

        static Parent of(AbstractJetRDD rdd, Partitioning edgePartitioner) {
            return new Parent(rdd, edgePartitioner);
        }
    }

}
