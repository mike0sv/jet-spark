package org.hazelcast.spark.utils;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.ProcessorSupplier;
import lombok.AllArgsConstructor;

import javax.annotation.Nonnull;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

/**
 * Created by mike on 17-Apr-17.
 */
@AllArgsConstructor
public class JetPeek<T> extends AbstractProcessor {
    private Distributed.Consumer<T> consumer;
    private boolean emit;

    public static ProcessorSupplier println() {
        return foreach(System.out::println);
    }

    public static <T> ProcessorSupplier foreach(Distributed.Consumer<T> consumer) {
        return peek(consumer, false);
    }

    public static <T> ProcessorSupplier peek(Distributed.Consumer<T> consumer) {
        return peek(consumer, true);
    }

    private static <T> ProcessorSupplier peek(Distributed.Consumer<T> consumer, boolean emit) {

        return (count) -> range(0, count).mapToObj(i -> new JetPeek<>(consumer, emit)).collect(toList());
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        consumer.accept(((T) item));
        if (emit) {
            emit(0, item);
        }
        return true;
    }
}
