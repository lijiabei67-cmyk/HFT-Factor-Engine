package com.yourorg.lob.mr.job1;

import com.yourorg.lob.factor.FactorContext;
import com.yourorg.lob.factor.FastAlphaComputer;
import com.yourorg.lob.model.DayTimeKey;
import com.yourorg.lob.model.FactorVectorWritable;
import com.yourorg.lob.model.SnapshotWritable;
import com.yourorg.lob.model.StockTimeKey;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FactorPerStockReducer extends Reducer<StockTimeKey, SnapshotWritable, DayTimeKey, FactorVectorWritable> {

    private FactorContext fctx;

    private final DayTimeKey outKey = new DayTimeKey();
    private final FactorVectorWritable outVal = new FactorVectorWritable();
    private final double[] vec = new double[20];

    private final SnapshotWritable prev = new SnapshotWritable();
    private boolean hasPrev;
    private double prevRatio = 0.0; // caching (sb/sa) from previous step

    @Override
    protected void setup(Context ctx) {
        fctx = FactorContext.from(ctx.getConfiguration());
    }

    @Override
    protected void reduce(StockTimeKey groupKey, Iterable<SnapshotWritable> values, Context ctx)
            throws IOException, InterruptedException {
        hasPrev = false;
        prevRatio = 0.0;

        for (SnapshotWritable cur : values) {
            int t = cur.intTime();

            double currentRatio = FastAlphaComputer.computeAllOptimized(cur, hasPrev ? prev : null, hasPrev ? prevRatio : 0.0, fctx, vec);

            if (t >= 93000 && t <= 150000) {
                outKey.set(cur.tradingDay(), t);
                outVal.setFrom(vec, 1L);
                ctx.write(outKey, outVal);
            }
            prev.copyFrom(cur);
            prevRatio = currentRatio;
            hasPrev = true;
        }
    }
    }
