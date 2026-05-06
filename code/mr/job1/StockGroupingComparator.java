package com.yourorg.lob.mr.job1;

import com.yourorg.lob.model.StockTimeKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StockGroupingComparator extends WritableComparator {

    public StockGroupingComparator() {
        super(StockTimeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        StockTimeKey x = (StockTimeKey) a;
        StockTimeKey y = (StockTimeKey) b;

        // 比较交易日
        int c = Integer.compare(x.tradingDay(), y.tradingDay());
        if (c != 0) return c;

        return Integer.compare(x.code(), y.code());
    }
}
