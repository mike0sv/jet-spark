package org.hazelcast.spark.rdd;

import com.hazelcast.jet.config.JetConfig;
import org.hazelcast.spark.JetSparkContext;

import java.util.*;
import java.util.logging.Logger;

/**
 * Created by mike on 16-Apr-17.
 */
@SuppressWarnings("unchecked")
public class AbstractJetRDDTest {
    static Logger LOGGER = Logger.getLogger("RDDTestLogger");
    static JetSparkContext context;
    static {
        LOGGER.info("Starting up");
        System.setProperty("hazelcast.logging.type", "none");
        JetConfig config = new JetConfig();
        config.getInstanceConfig().setCooperativeThreadCount(1);
        context = new JetSparkContext(config);
//        Jet.newJetInstance(); // because we can
        LOGGER.info("Done");
    }
    static List<String> cityList = Arrays.asList("Moscow", "St. P", "London", "New York", "LA", "Washington");
    static List<String> capitals = Arrays.asList("Moscow", "London", "Washington");
    static Map<String, String> capitalsMap = new HashMap() {{
        put("Russia", "Moscow");
        put("England", "London");
        put("USA", "Washington");
    }};
    static Map<String, String> countriesMap = new HashMap() {{
        put("Moscow", "Russia");
        put("St. P", "Russia");
        put("London", "England");
        put("New York", "USA");
        put("LA", "USA");
        put("Washington", "USA");
    }};

    static JetRDD<String> cityRDD = context.parallelize(cityList);
//    static JetPairRDDOld<String, String> capitalsPairRDD = context.parallelize(capitalsMap);

    static JetPairRDD<String, String> countriesPairRDD = context.parallelize(countriesMap);
    static JetPairRDDOld<String, String> countriesPairRDDOld = context.parallelizeOld(countriesMap);

    <E>boolean setEquals(Collection<E> first, Collection<E> second) {
        return new HashSet<>(first).equals(new HashSet<E>(second));
    }
}