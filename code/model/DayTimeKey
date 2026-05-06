package com.yourorg.lob.model;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DayTimeKey implements WritableComparable<DayTimeKey> {

    private int tradingDay;
    private int tradeTime; 

    public DayTimeKey() {}

    public DayTimeKey(int tradingDay, int tradeTime) {
        this.tradingDay = tradingDay;
        this.tradeTime = tradeTime;
    }

    public void set(int tradingDay, int tradeTime) {
        this.tradingDay = tradingDay;
        this.tradeTime = tradeTime;
    }

    public int tradingDay() { return tradingDay; }

    
    public int tradeTime() { return tradeTime; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tradingDay);
        out.writeInt(tradeTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tradingDay = in.readInt();
        this.tradeTime = in.readInt();
    }

    @Override
    public int compareTo(DayTimeKey o) {
        if (this.tradingDay != o.tradingDay) {
            return Integer.compare(this.tradingDay, o.tradingDay);
        }
        return Integer.compare(this.tradeTime, o.tradeTime);
    }

    @Override
    public int hashCode() {
        return tradingDay * 31 + tradeTime;
    }

    @Override
    public String toString() {
        return tradingDay + "\t" + tradeTime;
    }

    // 注册原生字节比较器 (加速排序)
    static {
        WritableComparator.define(DayTimeKey.class, new Comparator());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(DayTimeKey.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int day1 = readInt(b1, s1);
            int day2 = readInt(b2, s2);
            if (day1 != day2) return (day1 < day2) ? -1 : 1;

            int t1 = readInt(b1, s1 + 4);
            int t2 = readInt(b2, s2 + 4);
            return (t1 < t2) ? -1 : (t1 == t2 ? 0 : 1);
        }
    }
}
