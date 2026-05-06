import java.io.IOException;

public class FactorVectorWritable implements Writable {
    private final double[] sum = new double[20];
    private long count;

    public FactorVectorWritable() {}

    public FactorVectorWritable(double[] vec, long count) {
        setFrom(vec, count);
    }

    // 复用赋值 (Unrolled)
    public void setFrom(double[] vec, long count) {
        this.sum[0] = vec[0]; this.sum[1] = vec[1]; this.sum[2] = vec[2]; this.sum[3] = vec[3]; this.sum[4] = vec[4];
        this.sum[5] = vec[5]; this.sum[6] = vec[6]; this.sum[7] = vec[7]; this.sum[8] = vec[8]; this.sum[9] = vec[9];
        this.sum[10] = vec[10]; this.sum[11] = vec[11]; this.sum[12] = vec[12]; this.sum[13] = vec[13]; this.sum[14] = vec[14];
        this.sum[15] = vec[15]; this.sum[16] = vec[16]; this.sum[17] = vec[17]; this.sum[18] = vec[18]; this.sum[19] = vec[19];
        this.count = count;
    }

    //原地累加，不创建新对象，且循环展开
    public void add(FactorVectorWritable other) {
        this.sum[0] += other.sum[0]; this.sum[1] += other.sum[1]; this.sum[2] += other.sum[2]; this.sum[3] += other.sum[3]; this.sum[4] += other.sum[4];
        this.sum[5] += other.sum[5]; this.sum[6] += other.sum[6]; this.sum[7] += other.sum[7]; this.sum[8] += other.sum[8]; this.sum[9] += other.sum[9];
        this.sum[10] += other.sum[10]; this.sum[11] += other.sum[11]; this.sum[12] += other.sum[12]; this.sum[13] += other.sum[13]; this.sum[14] += other.sum[14];
        this.sum[15] += other.sum[15]; this.sum[16] += other.sum[16]; this.sum[17] += other.sum[17]; this.sum[18] += other.sum[18]; this.sum[19] += other.sum[19];
        this.count += other.count;
    }

    // 重置
    public void reset() {
        // 利用 JVM 优化，new double[20] 可能比循环赋值 0 快，或者直接 fill
        java.util.Arrays.fill(this.sum, 0.0);
        this.count = 0;
    }

    public double[] mean(double eps) {
        double denom = (double) count + eps;
        // 倒数乘法优化
        double inv = 1.0 / denom;
        double[] m = new double[20];
        // 循环展开
        m[0] = sum[0] * inv; m[1] = sum[1] * inv; m[2] = sum[2] * inv; m[3] = sum[3] * inv; m[4] = sum[4] * inv;
        m[5] = sum[5] * inv; m[6] = sum[6] * inv; m[7] = sum[7] * inv; m[8] = sum[8] * inv; m[9] = sum[9] * inv;
        m[10] = sum[10] * inv; m[11] = sum[11] * inv; m[12] = sum[12] * inv; m[13] = sum[13] * inv; m[14] = sum[14] * inv;
        m[15] = sum[15] * inv; m[16] = sum[16] * inv; m[17] = sum[17] * inv; m[18] = sum[18] * inv; m[19] = sum[19] * inv;
        return m;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 循环展开写入
        out.writeDouble(sum[0]); out.writeDouble(sum[1]); out.writeDouble(sum[2]); out.writeDouble(sum[3]); out.writeDouble(sum[4]);
        out.writeDouble(sum[5]); out.writeDouble(sum[6]); out.writeDouble(sum[7]); out.writeDouble(sum[8]); out.writeDouble(sum[9]);
        out.writeDouble(sum[10]); out.writeDouble(sum[11]); out.writeDouble(sum[12]); out.writeDouble(sum[13]); out.writeDouble(sum[14]);
        out.writeDouble(sum[15]); out.writeDouble(sum[16]); out.writeDouble(sum[17]); out.writeDouble(sum[18]); out.writeDouble(sum[19]);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum[0] = in.readDouble(); sum[1] = in.readDouble(); sum[2] = in.readDouble(); sum[3] = in.readDouble(); sum[4] = in.readDouble();
        sum[5] = in.readDouble(); sum[6] = in.readDouble(); sum[7] = in.readDouble(); sum[8] = in.readDouble(); sum[9] = in.readDouble();
        sum[10] = in.readDouble(); sum[11] = in.readDouble(); sum[12] = in.readDouble(); sum[13] = in.readDouble(); sum[14] = in.readDouble();
        sum[15] = in.readDouble(); sum[16] = in.readDouble(); sum[17] = in.readDouble(); sum[18] = in.readDouble(); sum[19] = in.readDouble();
        count = in.readLong();
    }
}
