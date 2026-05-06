package com.yourorg.lob.model;

public class Snapshot {

    private int tradingDay;     // 例如 20240115
    private String code;        // 证券代码
    private String tradeTime;   // 原样保存（例如 09:30:00 或 093000000）

    // L1~L10
    private final double[] bp = new double[10]; // bid price
    private final double[] ap = new double[10]; // ask price
    private final double[] bv = new double[10]; // bid volume
    private final double[] av = new double[10]; // ask volume

    private double tBidVol; // total bid volume（如果你们源数据有）
    private double tAskVol;

    public int tradingDay() { return tradingDay; }
    public String code() { return code; }
    public String tradeTime() { return tradeTime; }

    public double bp(int level1to10) { return bp[level1to10 - 1]; }
    public double ap(int level1to10) { return ap[level1to10 - 1]; }
    public double bv(int level1to10) { return bv[level1to10 - 1]; }
    public double av(int level1to10) { return av[level1to10 - 1]; }

    public double tBidVol() { return tBidVol; }
    public double tAskVol() { return tAskVol; }

    public void setTradingDay(int tradingDay) { this.tradingDay = tradingDay; }
    public void setCode(String code) { this.code = code; }
    public void setTradeTime(String tradeTime) { this.tradeTime = tradeTime; }

    public void setBp(int level1to10, double v) { bp[level1to10 - 1] = v; }
    public void setAp(int level1to10, double v) { ap[level1to10 - 1] = v; }
    public void setBv(int level1to10, double v) { bv[level1to10 - 1] = v; }
    public void setAv(int level1to10, double v) { av[level1to10 - 1] = v; }

    public void setTBidVol(double tBidVol) { this.tBidVol = tBidVol; }
    public void setTAskVol(double tAskVol) { this.tAskVol = tAskVol; }

    private int intCode;
    private int intTime;
    public void setIntCode(int c) { this.intCode = c; }
    public void setIntTime(int t) { this.intTime = t; }
    public int intCode() { return intCode; }
    public int intTime() { return intTime; }
}
