package org.hazelcast.spark.utils.connector;
import com.hazelcast.core.Member;
import com.hazelcast.jet.*;
import com.hazelcast.nio.Address;
import lombok.*;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.stream.IntStream.range;


public class ReadJdbcP<R> extends AbstractProcessor {
    private final Traverser<R> trav;


    private ReadJdbcP(Range range, JdbcHelper helper, @Nonnull Distributed.Function<ArrayList<Object>, R> mapper) {
        this.trav = traverseStream(helper.getRows(range).map(mapper));
    }

    @Override
    public boolean complete() {
        return emitCooperatively(trav);
    }

    public static <R> MetaSupplier<R> readJdbc(String url, String user, String password, String table, String id, List<String> fields,
                                               Distributed.Function<ArrayList<Object>, R> mapper) {
        JdbcHelper helper = new JdbcHelper(url, user, password, table, id, fields);
        return new MetaSupplier<R>(helper, mapper);
    }

    @AllArgsConstructor
    @Data
    private static class Range {
        private Long min;
        private Long max;
    }


    @RequiredArgsConstructor
    private static class MetaSupplier<R> implements ProcessorMetaSupplier {
        @NonNull JdbcHelper helper;
        @NonNull Distributed.Function<ArrayList<Object>, R> mapper;

        transient HashMap<Address, LinkedList<Range>> index = new HashMap<>();

        @Override
        public void init(@Nonnull Context context) {
            int totalParallelism = context.totalParallelism();
            Range minMax = helper.getMinMax();
            Long max = minMax.getMax() + 1;
            long batchSize = 1 + (max - minMax.getMin()) / totalParallelism;
            List<Address> addresses = context.jetInstance().getHazelcastInstance()
                    .getCluster().getMembers().stream().map(Member::getAddress)
                    .peek(x -> index.put(x, new LinkedList<>()))
                    .collect(Collectors.toList());

            for (int i = 0; i < addresses.size(); i++) {
                Address key = addresses.get(i);
                Long min = minMax.getMin() + i * batchSize;
                while (min < max) {
                    index.get(key).add(new Range(min, Math.min(max + 1, min + batchSize)));
                    min += batchSize * addresses.size();
                }
            }
        }

        @Nonnull
        @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return (a) -> new Supplier<>(index.get(a), helper, mapper);
        }
    }

    @AllArgsConstructor
    private static class Supplier<R> implements ProcessorSupplier {
        LinkedList<Range> ranges;
        JdbcHelper helper;
        Distributed.Function<ArrayList<Object>, R> mapper;

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            if (count != ranges.size()) {
                throw new RuntimeException("Count(localParallelism) is not equal to number of ranges. Maybe that was wrong assumption");
            }
            return ranges.stream().map(x -> new ReadJdbcP<R>(x, helper, mapper)).collect(Collectors.toList());
        }

        @Override
        public void complete(Throwable error) {

        }
    }

    @AllArgsConstructor
    private static class JdbcHelper {
        private String url;
        private String user;
        private String password;
        private String table;
        private String id;
        private List<String> fields;

        @SneakyThrows
        public Connection getConnection() {
            return DriverManager.getConnection(url, user, password);
        }

        @SneakyThrows
        public Range getMinMax() {
            String query = "SELECT MIN(" + id + "), MAX(" + id + ") FROM " + table;
            try (Connection connection = getConnection();
                 PreparedStatement ps = connection.prepareStatement(query)) {
                ResultSet resultSet = ps.executeQuery();
                if (resultSet.next()) {
                    return new Range(resultSet.getLong(1), resultSet.getLong(2));
                } else {
                    throw new RuntimeException("Could not get min and max from column " + id + " from table " + table);
                }
            }
        }

        @SneakyThrows
        public Stream<ArrayList<Object>> getRows(Range range) {
            String query = "SELECT " + String.join(", ", fields) +
                    " FROM " + table + " WHERE " + id + " >= ? AND " + id + " < ?";
            List<ArrayList<Object>> result = new LinkedList<>();
            try (Connection connection = getConnection();
                 PreparedStatement ps = connection.prepareStatement(query)) {
                ps.setLong(1, range.getMin());
                ps.setLong(2, range.getMax());
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    result.add(new ArrayList<>(
                            range(0, resultSet.getMetaData().getColumnCount())
                            .mapToObj(
                                    i -> uncheckCall(
                                            () -> resultSet.getObject(i + 1)))
                                    .collect(Collectors.toList())));
                }
            }
            System.out.println("Fetched " + result.size() + " rows");
            return result.stream();
        }

    }
}
