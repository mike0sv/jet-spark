package org.hazelcast.spark;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.jet.stream.IStreamMap;
import lombok.Getter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.hazelcast.spark.rdd.JetPairRDDOld;
import org.hazelcast.spark.rdd.JetPairRDD;
import org.hazelcast.spark.rdd.JetRDD;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.Processors.readList;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.connector.hadoop.ReadHdfsP.readHdfs;


@SuppressWarnings("unused")
public class JetSparkContext {
    @Getter
    protected JetInstance jet;
    private AtomicInteger counter = new AtomicInteger(0);

    public JetSparkContext() {
        jet = Jet.newJetInstance();
    }
    public JetSparkContext(JetConfig config) {
        jet = Jet.newJetInstance(config);
    }

    public <T> JetRDD<T> parallelize(Collection<T> list) {
        String name = getName("rdd");
        IStreamList<T> dlist = jet.getList(name);
        dlist.addAll(list);
        Vertex vertex = new Vertex(name, readList(name)).localParallelism(1);
        return new JetRDD<>(this, vertex);
    }

    public JetRDD<String> textFile(String path) {
        String name = getName("textFile");
        JobConf jobConfig = new JobConf();
        jobConfig.setInputFormat(TextInputFormat.class);

        TextInputFormat.addInputPath(jobConfig, new Path(path));

        Vertex vertex = new Vertex(name, readHdfs(jobConfig));
        return new JetRDD<>(this, vertex);
    }


    @Deprecated
    public <K, V> JetPairRDDOld<K, V> parallelizeOld(Map<K, V> map) {
        String name = getName("pairRdd");
        IStreamMap<K, V> dmap = jet.getMap(name);
        dmap.putAll(map);
        Vertex vertex = new Vertex(name, readMap(name));
        return new JetPairRDDOld<>(this, vertex);
    }

    public <K, V> JetPairRDD<K, V> parallelize(Map<K, V> map) {
        String name = getName("pairRdd");
        IStreamMap<K, V> dmap = jet.getMap(name);
        dmap.putAll(map);
        Vertex vertex = new Vertex(name, readMap(name));
        return new JetPairRDD<>(this, vertex);
    }

    public String getName(String prefix) {
//        return prefix + "_" + counter.getAndAdd(1);
        StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        String[] split = element.getClassName().split("\\.");
        String e = split[split.length - 2] + "." + element.getMethodName() +
                "(" + element.getFileName() + ":" + element.getLineNumber() + ")";
        return prefix + "_" + counter.getAndAdd(1) + " at " + e;
    }

    public void close() {
        jet.shutdown();
    }


}
