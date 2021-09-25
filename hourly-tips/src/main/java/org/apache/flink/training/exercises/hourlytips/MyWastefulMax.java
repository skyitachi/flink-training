package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;

public class MyWastefulMax extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {


    @Override
    public void process(Long key, /* driver id */
                        Context context,
                        Iterable<TaxiFare> elements,
                        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        float sum = 0F;
        for(TaxiFare taxiFare: elements) {
            sum += taxiFare.tip;
        }
        out.collect(Tuple3.of(context.window().getEnd(), key, sum));
    }
}
