package com.yourorg.lob.model;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SnapshotWritable implements Writable {

    private int tradingDay;
    private int intTime;

    // 配合 Mapper 传值用的临时字段 (不序列化)
    private int intCode;

    private final double[] bp = new double[10];
    private final double[] ap = new double[10];
    private final double[] bv = new double[10];
    private final double[] av = new double[10];
    private double tBidVol;
    private double tAskVol;

    public SnapshotWritable() {}

    // Getters
    public int tradingDay() { return tradingDay; }
    public int intTime() { return intTime; }
    public int intCode() { return intCode; }
    public double bp(int i) { return bp[i-1]; }
    public double ap(int i) { return ap[i-1]; }
    public double bv(int i) { return bv[i-1]; }
    public double av(int i) { return av[i-1]; }
    public double tBidVol() { return tBidVol; }
    public double tAskVol() { return tAskVol; }

    // Setters (Parser Direct Access)
    public void setTradingDay(int v) { this.tradingDay = v; }
    public void setIntTime(int v) { this.intTime = v; }
    public void setIntCode(int v) { this.intCode = v; } // 临时存储
    public void setTBidVol(double v) { this.tBidVol = v; }
    public void setTAskVol(double v) { this.tAskVol = v; }

    // Level Setters
    public void setBp(int level, double v) { this.bp[level-1] = v; }
    public void setAp(int level, double v) { this.ap[level-1] = v; }
    public void setBv(int level, double v) { this.bv[level-1] = v; }
    public void setAv(int level, double v) { this.av[level-1] = v; }

    // 复用逻辑
    public void copyFrom(SnapshotWritable other) {
        this.tradingDay = other.tradingDay;
        this.intTime = other.intTime;
        System.arraycopy(other.bp, 0, this.bp, 0, 10);
        System.arraycopy(other.ap, 0, this.ap, 0, 10);
        System.arraycopy(other.bv, 0, this.bv, 0, 10);
        System.arraycopy(other.av, 0, this.av, 0, 10);
        this.tBidVol = other.tBidVol;
        this.tAskVol = other.tAskVol;
    }

    public void setFromSnapshotWithNormalizedTime(Snapshot s, int normalizedIntTime) {
        // 兼容旧接口，这里其实用不到了，但为了不报错保留
        this.tradingDay = s.tradingDay();
        this.intTime = normalizedIntTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tradingDay);
        out.writeInt(intTime);
        // 手动展开，减少循环开销
        out.writeDouble(bp[0]); out.writeDouble(bp[1]); out.writeDouble(bp[2]); out.writeDouble(bp[3]); out.writeDouble(bp[4]);
        out.writeDouble(bp[5]); out.writeDouble(bp[6]); out.writeDouble(bp[7]); out.writeDouble(bp[8]); out.writeDouble(bp[9]);
        out.writeDouble(ap[0]); out.writeDouble(ap[1]); out.writeDouble(ap[2]); out.writeDouble(ap[3]); out.writeDouble(ap[4]);
        out.writeDouble(ap[5]); out.writeDouble(ap[6]); out.writeDouble(ap[7]); out.writeDouble(ap[8]); out.writeDouble(ap[9]);
        out.writeDouble(bv[0]); out.writeDouble(bv[1]); out.writeDouble(bv[2]); out.writeDouble(bv[3]); out.writeDouble(bv[4]);
        out.writeDouble(bv[5]); out.writeDouble(bv[6]); out.writeDouble(bv[7]); out.writeDouble(bv[8]); out.writeDouble(bv[9]);
        out.writeDouble(av[0]); out.writeDouble(av[1]); out.writeDouble(av[2]); out.writeDouble(av[3]); out.writeDouble(av[4]);
        out.writeDouble(av[5]); out.writeDouble(av[6]); out.writeDouble(av[7]); out.writeDouble(av[8]); out.writeDouble(av[9]);
        out.writeDouble(tBidVol);
        out.writeDouble(tAskVol);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tradingDay = in.readInt();
        this.intTime = in.readInt();
        bp[0] = in.readDouble(); bp[1] = in.readDouble(); bp[2] = in.readDouble(); bp[3] = in.readDouble(); bp[4] = in.readDouble();
        bp[5] = in.readDouble(); bp[6] = in.readDouble(); bp[7] = in.readDouble(); bp[8] = in.readDouble(); bp[9] = in.readDouble();
        ap[0] = in.readDouble(); ap[1] = in.readDouble(); ap[2] = in.readDouble(); ap[3] = in.readDouble(); ap[4] = in.readDouble();
        ap[5] = in.readDouble(); ap[6] = in.readDouble(); ap[7] = in.readDouble(); ap[8] = in.readDouble(); ap[9] = in.readDouble();
        bv[0] = in.readDouble(); bv[1] = in.readDouble(); bv[2] = in.readDouble(); bv[3] = in.readDouble(); bv[4] = in.readDouble();
        bv[5] = in.readDouble(); bv[6] = in.readDouble(); bv[7] = in.readDouble(); bv[8] = in.readDouble(); bv[9] = in.readDouble();
        av[0] = in.readDouble(); av[1] = in.readDouble(); av[2] = in.readDouble(); av[3] = in.readDouble(); av[4] = in.readDouble();
        av[5] = in.readDouble(); av[6] = in.readDouble(); av[7] = in.readDouble(); av[8] = in.readDouble(); av[9] = in.readDouble();
        this.tBidVol = in.readDouble();
        this.tAskVol = in.readDouble();
    }
}
