package com.yourorg.lob.mr.job2;

import com.yourorg.lob.model.DayTimeKey;
import com.yourorg.lob.model.FactorVectorWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CrossSectionAvgMapper extends Mapper<DayTimeKey, FactorVectorWritable, DayTimeKey, FactorVectorWritable> {
    @Override
    protected void map(DayTimeKey key, FactorVectorWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
