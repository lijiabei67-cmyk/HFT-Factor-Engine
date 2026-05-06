package com.yourorg.lob.mr.job2;

import com.yourorg.lob.model.DayTimeKey;
import com.yourorg.lob.model.FactorVectorWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CrossSectionAvgCombiner extends Reducer<DayTimeKey, FactorVectorWritable, DayTimeKey, FactorVectorWritable> {

    // 复用输出对象
    private final FactorVectorWritable agg = new FactorVectorWritable();

    @Override
    protected void reduce(DayTimeKey key, Iterable<FactorVectorWritable> values, Context context)
            throws IOException, InterruptedException {

        // 1. 重置累加器 (不 new 新对象)
        agg.reset();

        // 2. 原地累加
        for (FactorVectorWritable v : values) {
            agg.add(v);
        }

        // 3. 写出
        context.write(key, agg);
    }
}
