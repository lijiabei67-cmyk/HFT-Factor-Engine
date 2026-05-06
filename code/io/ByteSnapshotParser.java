package com.yourorg.lob.io;

import com.yourorg.lob.model.SnapshotWritable;
import org.apache.hadoop.io.Text;
import java.util.HashMap;
import java.util.Map;

public class ByteSnapshotParser {
    private final int[] commaPos = new int[128];
    private int[] fieldIndices = null;

   
    public boolean initIndices(String headerLine) {
        if (headerLine == null || !headerLine.contains("tradingDay")) return false;
        String[] headers = headerLine.trim().split(",");
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            map.put(headers[i].trim(), i);
        }
        fieldIndices = new int[5 + 40];
        fieldIndices[0] = map.getOrDefault("tradingDay", -1);
        fieldIndices[1] = map.getOrDefault("code", -1);
        fieldIndices[2] = map.getOrDefault("tradeTime", -1);
        fieldIndices[3] = map.getOrDefault("tBidVol", -1);
        fieldIndices[4] = map.getOrDefault("tAskVol", -1);
        int cursor = 5;
        for (int i = 1; i <= 10; i++) {
            fieldIndices[cursor++] = map.getOrDefault("bp" + i, -1);
            fieldIndices[cursor++] = map.getOrDefault("bv" + i, -1);
            fieldIndices[cursor++] = map.getOrDefault("ap" + i, -1);
            fieldIndices[cursor++] = map.getOrDefault("av" + i, -1);
        }
        return true;
    }

    public boolean isInitialized() { return fieldIndices != null; }

    public boolean parse(Text line, SnapshotWritable out) {
        if (line.getLength() == 0) return false;

        byte[] bytes = line.getBytes();
        int len = line.getLength();

        int fieldCount = 0;
        commaPos[fieldCount++] = -1;
        for (int i = 0; i < len; i++) {
            if (bytes[i] == ',') {
                if (fieldCount >= 127) break;
                commaPos[fieldCount++] = i;
            }
        }
        commaPos[fieldCount] = len;

        if (fieldCount < 20) return false;

        // tradingDay & code
        int tDayIdx = fieldIndices[0];
        if (tDayIdx >= 0) out.setTradingDay(parseInt(bytes, getStart(tDayIdx), getEnd(tDayIdx)));

        // time (解析为 int)
        int timeIdx = fieldIndices[2];
        if (timeIdx >= 0) out.setIntTime(parseTimeToInt(bytes, getStart(timeIdx), getEnd(timeIdx)));

        // volumes
        int tbIdx = fieldIndices[3];
        if (tbIdx >= 0) out.setTBidVol(parseDoubleEx(bytes, getStart(tbIdx), getEnd(tbIdx)));

        int taIdx = fieldIndices[4];
        if (taIdx >= 0) out.setTAskVol(parseDoubleEx(bytes, getStart(taIdx), getEnd(taIdx)));

        int base = 5;
        // L1
        fillLevel(out, bytes, base, 1); base += 4;
        fillLevel(out, bytes, base, 2); base += 4;
        fillLevel(out, bytes, base, 3); base += 4;
        fillLevel(out, bytes, base, 4); base += 4;
        fillLevel(out, bytes, base, 5); base += 4;
        fillLevel(out, bytes, base, 6); base += 4;
        fillLevel(out, bytes, base, 7); base += 4;
        fillLevel(out, bytes, base, 8); base += 4;
        fillLevel(out, bytes, base, 9); base += 4;
        fillLevel(out, bytes, base, 10);

        return true;
    }

    // 辅助内联方法
    private void fillLevel(SnapshotWritable out, byte[] b, int baseIdx, int level) {
        // 检查索引越界
        // indices: base, base+1, base+2, base+3
        // 对应: bp, bv, ap, av
        int idx1 = fieldIndices[baseIdx];
        if (idx1 >= 0) out.setBp(level, parseDoubleEx(b, getStart(idx1), getEnd(idx1)));

        int idx2 = fieldIndices[baseIdx+1];
        if (idx2 >= 0) out.setBv(level, parseDoubleEx(b, getStart(idx2), getEnd(idx2)));

        int idx3 = fieldIndices[baseIdx+2];
        if (idx3 >= 0) out.setAp(level, parseDoubleEx(b, getStart(idx3), getEnd(idx3)));

        int idx4 = fieldIndices[baseIdx+3];
        if (idx4 >= 0) out.setAv(level, parseDoubleEx(b, getStart(idx4), getEnd(idx4)));
    }

    private int getStart(int colIdx) { return commaPos[colIdx] + 1; }
    private int getEnd(int colIdx) { return commaPos[colIdx + 1]; }

    // ... (保留之前的 parseInt, parseTimeToInt, parseDoubleEx 方法) ...
    // 请务必保留之前那个 parseDoubleEx，它是提速的核心！

    private static int parseInt(byte[] b, int start, int end) {
        int ret = 0;
        for (int i = start; i < end; i++) ret = ret * 10 + (b[i] - '0');
        return ret;
    }

    private static int parseTimeToInt(byte[] b, int start, int end) {
        if (start >= end) return 0;
        int val = 0, count = 0;
        for (int i = start; i < end; i++) {
            if (b[i] >= '0' && b[i] <= '9') {
                val = val * 10 + (b[i] - '0');
                if (++count == 6) break;
            }
        }
        return val;
    }

    private static double parseDoubleEx(byte[] b, int start, int end) {
        if (start >= end) return 0.0;
        long integerPart = 0;
        int i = start;
        for (; i < end; i++) {
            byte c = b[i];
            if (c == '.') break;
            integerPart = integerPart * 10 + (c - '0');
        }
        if (i == end) return (double) integerPart;
        i++;
        double fractionPart = 0;
        double divisor = 1.0;
        for (; i < end; i++) {
            fractionPart = fractionPart * 10 + (b[i] - '0');
            divisor *= 10.0;
        }
        return integerPart + (fractionPart / divisor);
    }
}
