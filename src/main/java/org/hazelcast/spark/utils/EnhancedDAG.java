package org.hazelcast.spark.utils;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@SuppressWarnings("unused")
public class EnhancedDAG extends DAG {
    private LinkedList<Runnable> executionStartedCallbacks = new LinkedList<>();
    private LinkedList<Runnable> executionEndedCallbacks = new LinkedList<>();

    public void execute(JetInstance jet) throws ExecutionException, InterruptedException {
        Future<Void> future = jet.newJob(this).execute();
        executionStartedCallbacks.forEach(Runnable::run);
        future.get();
        executionEndedCallbacks.forEach(Runnable::run);
    }

    public void addJobStartedCallback(Runnable callback) {
        executionStartedCallbacks.add(callback);
    }

    public void addJobEndedCallback(Runnable callback) {
        executionEndedCallbacks.add(callback);
    }

    public Edge createEdge(Vertex source, Vertex destination, int destinationOrdinal) {
        List<Edge> edges = getOutboundEdges(source.getName());
        return Edge.from(source, edges.size()).to(destination, destinationOrdinal);
    }
}
