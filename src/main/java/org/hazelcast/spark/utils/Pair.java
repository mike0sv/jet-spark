package org.hazelcast.spark.utils;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by mike on 17-Apr-17.
 */
@AllArgsConstructor
@Data
public class Pair<T1, T2> implements Serializable {
    private final T1 _1;
    private final T2 _2;

    @Override
    public String toString() {
        return "(" + _1 + ", " + _2 + ")";
//        return "((" + _1.getClass() + ") " + _1 + ", (" + _2.getClass() + ") " + _2 + ")";
    }
}
