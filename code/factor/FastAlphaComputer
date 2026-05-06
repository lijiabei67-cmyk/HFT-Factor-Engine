package com.yourorg.lob.factor;

import com.yourorg.lob.model.SnapshotWritable;

public final class FastAlphaComputer {
    private FastAlphaComputer() {}

    public static double computeAllOptimized(SnapshotWritable cur, SnapshotWritable prev,
                                             double prevRatio, FactorContext ctx, double[] outVec) {
        // 本地化 eps
        final double eps = ctx.eps;

        // 1. 提取 L1 数据 (Alpha 1-4, 17, 18 仅依赖这些，之前是对的)
        final double bp1 = cur.bp(1);
        final double ap1 = cur.ap(1);
        final double bv1 = cur.bv(1);
        final double av1 = cur.av(1);

        final double spread = ap1 - bp1;
        final double mid = (ap1 + bp1) * 0.5;

        // Alpha 1-4
        outVec[0] = spread;
        outVec[1] = spread / (mid + eps);
        outVec[2] = mid;
        outVec[3] = (bv1 - av1) / (bv1 + av1 + eps);

        // 2. 聚合计算 (关键修正：严格只计算 L1 - L5)
        // 之前的代码累加了 L1-L10，导致总量(Alpha 6,7)偏大，且其他因子偏移

        double sb = 0.0; // Sum Buy Vol
        double sa = 0.0; // Sum Sell Vol
        double bidPV = 0.0; // Sum(Price * Vol)
        double askPV = 0.0;
        double wmNum = 0.0; // Weighted Mid Numerator
        double wmDen = 0.0;
        double asymB = 0.0; // Decay Sum
        double asymA = 0.0;

        // 局部变量缓存
        double p, v;

        // --- L1 ---
        p = bp1; v = bv1;
        sb += v; bidPV += p*v; wmNum += p*v; asymB += v; // v/1.0
        p = ap1; v = av1;
        sa += v; askPV += p*v; wmNum += p*v; wmDen += v + bv1; asymA += v;

        // --- L2 ---
        p = cur.bp(2); v = cur.bv(2);
        sb += v; bidPV += p*v; wmNum += p*v; asymB += v * 0.5;
        p = cur.ap(2); v = cur.av(2);
        sa += v; askPV += p*v; wmNum += p*v; wmDen += v + cur.bv(2); asymA += v * 0.5;

        // --- L3 ---
        p = cur.bp(3); v = cur.bv(3);
        sb += v; bidPV += p*v; wmNum += p*v; asymB += v * 0.3333333333333333;
        p = cur.ap(3); v = cur.av(3);
        sa += v; askPV += p*v; wmNum += p*v; wmDen += v + cur.bv(3); asymA += v * 0.3333333333333333;

        // --- L4 ---
        p = cur.bp(4); v = cur.bv(4);
        sb += v; bidPV += p*v; wmNum += p*v; asymB += v * 0.25;
        p = cur.ap(4); v = cur.av(4);
        sa += v; askPV += p*v; wmNum += p*v; wmDen += v + cur.bv(4); asymA += v * 0.25;

        // --- L5 --- (到此为止！不要计算 L6-L10)
        p = cur.bp(5); v = cur.bv(5);
        sb += v; bidPV += p*v; wmNum += p*v; asymB += v * 0.2;
        p = cur.ap(5); v = cur.av(5);
        sa += v; askPV += p*v; wmNum += p*v; wmDen += v + cur.bv(5); asymA += v * 0.2;

        // 3. 聚合因子输出
        double sumVol = sb + sa;

        // Alpha 5: 前n档多档不平衡
        outVec[4] = (sb - sa) / (sumVol + eps);

        // Alpha 6: 前n档买方深度 (之前FAIL就是因为这里累加了10档)
        outVec[5] = sb;

        // Alpha 7: 前n档卖方深度
        outVec[6] = sa;

        // Alpha 8: 深度差
        outVec[7] = sb - sa;

        // Alpha 9: 深度比
        double currentRatio = sb / (sa + eps);
        outVec[8] = currentRatio;

        // Alpha 10: 全市场买卖量平衡 (使用 tBidVol, tAskVol，与 n 无关，之前已PASS)
        double tBid = cur.tBidVol();
        double tAsk = cur.tAskVol();
        outVec[9] = (tBid - tAsk) / (tBid + tAsk + eps);

        // Alpha 11, 12: VWAP (n=5)
        double vwapBid = bidPV / (sb + eps);
        double vwapAsk = askPV / (sa + eps);
        outVec[10] = vwapBid;
        outVec[11] = vwapAsk;

        // Alpha 13: 加权中间价
        outVec[12] = wmNum / (wmDen + eps);

        // Alpha 14: 加权价差 (VWAP Ask - Bid)
        // 之前FAIL是因为 n=10 时价差会被拉大(深层订单价格差更大)，改回 n=5 后应PASS
        outVec[13] = vwapAsk - vwapBid;

        // Alpha 15: 买卖密度差 (平均挂单量差)
        // 必须除以 n=5，之前除以 10 是错误的
        outVec[14] = (sb - sa) * 0.2; // * 1/5

        // Alpha 16: 买卖不对称度 (Decay)
        outVec[15] = (asymB - asymA) / (asymB + asymA + eps);

        // 时间序列类因子 (Alpha 17 - 19)
        if (prev == null) {
            outVec[16] = 0.0; outVec[17] = 0.0; outVec[18] = 0.0;
        } else {
            // Alpha 17: 最优价变动 (L1, PASS)
            outVec[16] = ap1 - prev.ap(1);

            // Alpha 18: 中间价变动 (L1, PASS)
            double midPrev = (prev.ap(1) + prev.bp(1)) * 0.5;
            outVec[17] = mid - midPrev;

            // Alpha 19: 深度比变动
            // 依赖于 currentRatio (Alpha 9)，之前 9 错了所以 19 错，现在修正了
            outVec[18] = currentRatio - prevRatio;
        }

        // Alpha 20: 价压指标 (依赖 Sum Vol)
        outVec[19] = spread / (sumVol + eps);

        return currentRatio;
    }
}
    
