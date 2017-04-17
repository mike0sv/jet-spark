package org.hazelcast.spark.rdd.partitioning;

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Partitioner;
import com.hazelcast.jet.config.EdgeConfig;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;


/**
 * @param <T>
 * @param <K>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@SuppressWarnings("unused")
public class Partitioning<T, K> {
    private boolean buffered;
    private boolean distributed;
    private int priority;
    private boolean broadcast;
    private boolean allToOne;
    private EdgeConfig edgeConfig;
    private Distributed.Function<T, K> keyExtractor;
    private Partitioner<K> partitioner;

    public static Partitioning defaultPartitioning() {
        return new Partitioning<>();
    }

    public static <T, K> Partitioning<T, K> by(Distributed.Function<T, K> keyExtractor) {
        Partitioning<T, K> partitioning = new Partitioning<>();
        partitioning.keyExtractor = keyExtractor;
        return partitioning;
    }

    public static <T, K> Partitioning<T, K> by(Distributed.Function<T, K> keyExtractor, Partitioner<K> partitioner) {
        Partitioning<T, K> partitioning = new Partitioning<>();
        partitioning.keyExtractor = keyExtractor;
        partitioning.partitioner = partitioner;
        return partitioning;
    }

    public Partitioning<T, K> buffered() {
        buffered = true;
        return this;
    }

    public Partitioning<T, K> distributed() {
        distributed = true;
        return this;
    }

    public Partitioning<T, K> priority(int priority) {
        this.priority = priority;
        return this;
    }

    public Partitioning<T, K> edgeConfig(EdgeConfig edgeConfig) {
        this.edgeConfig = edgeConfig;
        return this;
    }

    public Partitioning<T, K> broadcast() {
        broadcast = true;
        return this;
    }

    public Partitioning<T, K> allToOne() {
        allToOne = true;
        return this;
    }

    public static Edge apply(Partitioning partitioning, Edge edge) {
        if (partitioning != null) {
            partitioning.apply(edge);
        }
        return edge;
    }

    public Edge apply(Edge edge) {
        if (buffered) {
            edge.buffered();
        }
        if (distributed) {
            edge.distributed();
        }
        if (broadcast) {
            edge.broadcast();
        }
        if (allToOne) {
            edge.allToOne();
        }
        edge.setConfig(edgeConfig);
        edge.priority(priority);
        if (keyExtractor != null) {
            if (partitioner == null) {
                edge.partitioned(keyExtractor);
            } else {
                edge.partitioned(keyExtractor, partitioner);
            }
        }
        return edge;
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder().append("Partitioning(");
        if (buffered) {
            builder.append("buffered ");
        }
        if (distributed) {
            builder.append("distributed ");
        }
        if (priority != 0) {
            builder.append("priority=").append(priority).append(" ");
        }
        if (broadcast) {
            builder.append("broadcast ");
        }
        if (allToOne) {
            builder.append("allToOne ");
        }
        if (edgeConfig != null) {
            builder.append("edgeConfig=").append(edgeConfig).append(" ");
        }
        if (keyExtractor != null) {
            builder.append("by key ");
        }
        if (partitioner != null) {
            builder.append("with partitioner");
        }
        builder.append(")");
        return builder.toString();
    }
}
