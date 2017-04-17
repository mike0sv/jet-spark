package org.hazelcast.spark.utils;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Traverser;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.jet.KeyExtractors.entryKey;
import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;

@SuppressWarnings("unused")
public class JetJoining {

    public static <K, T1, T2, R> BiFunction<Map.Entry<K, T1>, Map.Entry<K, T2>, Map.Entry<K, Pair<T1, T2>>> pairing() {
        return (e1, e2) -> entry(e1.getKey(), new Pair<T1, T2>(e1.getValue(), e2.getValue()));
    }
    public static ProcessorSupplier innerJoin() {
        return ProcessorSupplier.of(() -> new InnerJoin<>(entryKey(), entryKey(), pairing()));
    }

    public static ProcessorSupplier priorityInnerJoin() {
        return ProcessorSupplier.of(() -> new PriorityInnerJoin<>(entryKey(), entryKey(), pairing()));
    }

    @SuppressWarnings("unchecked")
    private static class JoinBase<K, T1, T2, R> extends AbstractProcessor {
        HashMap<K, Set<T1>> accumulator1 = new HashMap<>();
        HashMap<K, Set<T2>> accumulator2 = new HashMap<>();
        Function<? super T1, ? extends K> keyExtractor1;
        Function<? super T2, ? extends K> keyExtractor2;
        BiFunction<? super T1, ? super T2, ? extends R> finisher;
        Traverser<R> traverser;

        JoinBase(Function<? super T1, ? extends K> keyExtractor1,
                 Function<? super T2, ? extends K> keyExtractor2,
                 BiFunction<? super T1, ? super T2, ? extends R> finisher) {
            this.keyExtractor1 = keyExtractor1;
            this.keyExtractor2 = keyExtractor2;
            this.finisher = finisher;
            traverser = lazy(() -> traverseStream(
                    accumulator1.entrySet().stream()
                            .filter(e1 -> accumulator2.containsKey(e1.getKey()))
                            .flatMap(e1 -> e1.getValue().stream()
                                    .flatMap(t1 -> accumulator2.get(e1.getKey()).stream()
                                            .map(t2 -> finisher.apply(t1, t2))))));
        }

        <T> void collectValue(T item, HashMap<K, Set<T>> accumulator, Function<? super T, ? extends K> keyExtractor) {
            accumulator.compute(keyExtractor.apply(item),
                    (x, a) -> {
                        if (a == null) {
                            return Collections.singleton(item);
                        } else {
                            a.add(item);
                            return a;
                        }
                    });
        }

        @Override
        protected boolean tryProcess0(@Nonnull Object item) throws Exception {
//            System.out.println("collecting 1 " + item);
            collectValue((T1) item, accumulator1, keyExtractor1);
            return true;
        }

        @Override
        protected boolean tryProcess1(@Nonnull Object item) throws Exception {
//            System.out.println("collecting 2 " + item + " " + item.getClass());
            collectValue((T2) item, accumulator2, keyExtractor2);
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private static class InnerJoin<K, T1, T2, R> extends JoinBase<K, T1, T2, R> {

        InnerJoin(Function<? super T1, ? extends K> keyExtractor1, Function<? super T2, ? extends K> keyExtractor2, BiFunction<? super T1, ? super T2, ? extends R> finisher) {
            super(keyExtractor1, keyExtractor2, finisher);
        }

        @Override
        public boolean complete() {
//            System.out.println("Join complete, acc sizes " + accumulator1.size() + " " + accumulator2.size());
//            System.out.println(accumulator1 + "\n" + accumulator2);
            return emitCooperatively(traverser);
        }


    }


    private static class PriorityInnerJoin<K, T1, T2, R> extends InnerJoin<K, T1, T2, R> {

        PriorityInnerJoin(Function<? super T1, ? extends K> keyExtractor1, Function<? super T2, ? extends K> keyExtractor2, BiFunction<? super T1, ? super T2, ? extends R> finisher) {
            super(keyExtractor1, keyExtractor2, finisher);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected boolean tryProcess1(@Nonnull Object item) throws Exception {
            T2 t2 = (T2) item;
            K key = keyExtractor2.apply(t2);
            if (accumulator1.containsKey(key)) {
                for (T1 t1 : accumulator1.get(key)) {
                    emit(0, finisher.apply(t1, t2));
                }
            }
            return true;
        }

        @Override
        public boolean complete() {
            System.out.println("Join complete, acc sizes " + accumulator1.size() + " " + accumulator2.size());
            return true;
        }
    }
}
