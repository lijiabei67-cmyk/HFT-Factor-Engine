package com.yourorg.lob.model;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StockTimeKey implements WritableComparable<StockTimeKey> {
    private int tradingDay;
    private int code;
    private int tradeTime;

    public StockTimeKey() {}

    public void set(int tradingDay, int code, int tradeTime) {
        this.tradingDay = tradingDay;
        this.code = code;
        this.tradeTime = tradeTime;
    }

    public int tradingDay() { return tradingDay; }
    public int code() { return code; }
    public int tradeTime() { return tradeTime; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tradingDay);
        out.writeInt(code);
        out.writeInt(tradeTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tradingDay = in.readInt();
        this.code = in.readInt();
        this.tradeTime = in.readInt();
    }

    @Override
    public int compareTo(StockTimeKey o) {
        if (this.tradingDay != o.tradingDay) return Integer.compare(this.tradingDay, o.tradingDay);
        if (this.code != o.code) return Integer.compare(this.code, o.code);
        return Integer.compare(this.tradeTime, o.tradeTime);
    }

    @Override
    public int hashCode() {
        return tradingDay * 31 * 31 + code * 31 + tradeTime;
    }

    static {
        WritableComparator.define(StockTimeKey.class, new Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(StockTimeKey.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // 直接比较 12 个字节 (3个int)，跳过所有反序列化
            // Java 的大端序 int 字节序与数值大小顺序一致，可以直接按字节比
            return compareBytes(b1, s1, 12, b2, s2, 12);
        }
    }
}
