package com.yourorg.lob.mr.job1;

import com.yourorg.lob.model.SnapshotWritable;
import com.yourorg.lob.model.StockTimeKey;
import org.apache.hadoop.mapreduce.Partitioner;

public class StockPartitioner extends Partitioner<StockTimeKey, SnapshotWritable> {

    @Override
    public int getPartition(StockTimeKey key, SnapshotWritable value, int numPartitions) {
        // 混合 tradingDay 和 code 确保散列均匀
        int h = 31 * key.tradingDay() + key.code();

        return (h & Integer.MAX_VALUE) % numPartitions;
    }
}
