package com.yourorg.lob.mr.job2;

import com.yourorg.lob.model.DayTimeKey;
import com.yourorg.lob.model.FactorVectorWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.HashSet;

public class CrossSectionAvgReducer extends Reducer<DayTimeKey, FactorVectorWritable, NullWritable, Text> {
    private double eps;
    private MultipleOutputs<NullWritable, Text> mos;
    private final HashSet<String> headerWritten = new HashSet<>();
    private final Text outVal = new Text();

    // StringBuilder 缓冲区，适当加大一点防止扩容
    private final StringBuilder sb = new StringBuilder(512);

    // 复用聚合对象，避免 new
    private final FactorVectorWritable agg = new FactorVectorWritable();

    @Override
    protected void setup(Context ctx) {
        eps = ctx.getConfiguration().getDouble("lob.epsilon", 1e-7);
        mos = new MultipleOutputs<>(ctx);
    }

    @Override
    protected void reduce(DayTimeKey key, Iterable<FactorVectorWritable> values, Context context)
            throws IOException, InterruptedException {

        // 1. 原地聚合 (不 new 对象)
        agg.reset();
        for (FactorVectorWritable v : values) {
            agg.add(v);
        }

        // 2. 计算均值
        double[] mean = agg.mean(eps);

        // 3. 文件名生成 (MMDD.csv)
        // key.tradingDay() 是 int，例如 20240102
        // 快速提取后四位，避免 String.format
        int day = key.tradingDay();
        int mmddInt = day % 10000;

        // 手动构建文件名字符串，避免 Integer.toString 的部分开销
        // 假设月份是 01-12，不用担心不足4位的问题，除非是 1月2号 -> 102
        // 为了稳妥，还是用简单逻辑拼接
        String dayStr = Integer.toString(mmddInt);
        // 补齐 0 (例如 102 -> 0102)
        String file;
        if (mmddInt < 1000) {
            file = "0" + dayStr + ".csv";
        } else {
            file = dayStr + ".csv";
        }

        // --- 写表头 (Header) ---
        if (!headerWritten.contains(file)) {
            sb.setLength(0);
            sb.append("time");
            for (int i = 1; i <= 20; i++) sb.append(",alpha_").append(i);
            mos.write(NullWritable.get(), new Text(sb.toString()), file);
            headerWritten.add(file);
        }

        // --- 构建输出行 ---
        sb.setLength(0);

        // 4. 【关键修改】时间格式化: 93000 -> "093000" (不带冒号!)
        int t = key.tradeTime();
        if (t < 100000) {
            sb.append('0'); // 9点的时间补前导0
        }
        sb.append(t);

        // 5. 快速 Double 输出
        for (double v : mean) {
            sb.append(',');
            appendScaledDouble(sb, v);
        }

        outVal.set(sb.toString());
        mos.write(NullWritable.get(), outVal, file);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (mos != null) mos.close();
    }

    // 极速浮点格式化 (手动实现，避开 String.format)
    // 保留 6 位小数
    private void appendScaledDouble(StringBuilder sb, double val) {
        // 乘 10^6 四舍五入转 long
        long l = (long) (val * 1000000.0 + (val > 0 ? 0.5 : -0.5));

        if (l < 0) {
            sb.append('-');
            l = -l;
        }

        String s = Long.toString(l);
        int len = s.length();

        if (len > 6) {
            // 例如 1234567 -> 1.234567
            sb.append(s, 0, len - 6).append('.').append(s, len - 6, len);
        } else {
            // 例如 1234 -> 0.001234
            sb.append("0.");
            for (int i = 0; i < 6 - len; i++) sb.append('0');
            sb.append(s);
        }
    }
}
