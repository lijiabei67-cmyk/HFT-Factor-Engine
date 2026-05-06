package com.yourorg.lob.mr.job1;

import com.yourorg.lob.io.ByteSnapshotParser;
import com.yourorg.lob.model.SnapshotWritable;
import com.yourorg.lob.model.StockTimeKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FactorPerStockMapper extends Mapper<LongWritable, Text, StockTimeKey, SnapshotWritable> {

    private final ByteSnapshotParser parser = new ByteSnapshotParser();
    private final StockTimeKey outKey = new StockTimeKey();
    private final SnapshotWritable outVal = new SnapshotWritable();

    // 默认表头
    private static final String DEFAULT_HEADER = "tradingDay,tradeTime,recvtime,MIC,code,cumCut,cumVol,turnover,last,open,high,low,tBidVol,tAskVol,wBidPrc,wAskPrc,openInterest,bp1,bv1,ap1,av1,bp2,bv2,ap2,av2,bp3,bv3,ap3,av3,bp4,bv4,ap4,av4,bp5,bv5,ap5,av5,bp6,bv6,ap6,av6,bp7,bv7,ap7,av7,bp8,bv8,ap8,av8,bp9,bv9,ap9,av9,bp10,bv10,ap10,av10";

    @Override
    protected void setup(Context context) {
        parser.initIndices(DEFAULT_HEADER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        // 1. 表头检查
        if (value.getLength() > 10) {
            byte[] b = value.getBytes();
            // 检查 "tradingDay"
            if (b[0] == 't' && b[2] == 'a') {
                parser.initIndices(value.toString());
                return;
            }
        }

        // 2. 解析 (Zero-Copy) -> 填充 outVal
        if (!parser.parse(value, outVal)) {
            return;
        }

        // 3. 过滤
        int t = outVal.intTime();
        if (t < 92500 || t > 150000) return;

        // 4. 设置 Key
        // 使用独立逻辑快速提取 Code (避免去 Parser 里改太多逻辑)
        int code = parseCodeFast(value);
        outKey.set(outVal.tradingDay(), code, t);

        ctx.write(outKey, outVal);
    }

    // 极速提取 Code (假定 code 在第 5 列，即 index 4)
    private int parseCodeFast(Text line) {
        byte[] b = line.getBytes();
        int len = line.getLength();
        int commaCount = 0;
        int i = 0;

        // 扫描前 4 个逗号 (跳过 tradingDay, tradeTime, recvtime, MIC)
        // 注意：根据你的 CSV 实际列数调整。标准数据 code 通常在第 5 列。
        for (; i < len; i++) {
            if (b[i] == ',') {
                commaCount++;
                if (commaCount == 4) break;
            }
        }
        i++; // 跳过逗号

        int val = 0;
        for (; i < len; i++) {
            byte c = b[i];
            if (c == ',') break; // 遇到下一个逗号结束
            if (c >= '0' && c <= '9') val = val * 10 + (c - '0');
        }
        return val;
    }
}
