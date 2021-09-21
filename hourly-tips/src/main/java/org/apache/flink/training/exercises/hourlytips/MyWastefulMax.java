package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;
import java.util.Map;

public class MyWastefulMax extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {


    @Override
    public void process(Long key, /* driver id */
                        Context context,
                        Iterable<TaxiFare> elements,
                        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        long maxDriverId = 0L;
        Float maxTips = 0.0F;
        System.out.println("in the process");
        Map<Long, Float> sums = new HashMap<>();
        for(TaxiFare taxiFare: elements) {

            System.out.println("receive events: " + taxiFare.toString());

            sums.put(taxiFare.driverId, sums.getOrDefault(taxiFare.driverId, 0.0f) + taxiFare.tip);
            Float temp = sums.get(taxiFare.driverId);
            if (temp > maxTips) {
                maxTips = temp;
                maxDriverId = taxiFare.driverId;
            }
        }
        out.collect(Tuple3.of(context.window().getEnd(), maxDriverId, maxTips));
    }
}
