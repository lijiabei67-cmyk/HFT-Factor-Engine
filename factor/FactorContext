package com.yourorg.lob.factor;

import org.apache.hadoop.conf.Configuration;

public class FactorContext {
    public final int n;
    public final int dt;
    public final double eps;

    public FactorContext(int n, int dt, double eps) {
        this.n = n;
        this.dt = dt;
        this.eps = eps;
    }

    public static FactorContext from(Configuration conf) {
        int n = conf.getInt("lob.n", 5);
        int dt = conf.getInt("lob.dt", 1);
        double eps = conf.getDouble("lob.epsilon", 1e-7);
        return new FactorContext(n, dt, eps);
    }
}
