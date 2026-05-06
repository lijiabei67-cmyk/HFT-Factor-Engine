package com.yourorg.lob;

import com.yourorg.lob.model.DayTimeKey;
import com.yourorg.lob.model.FactorVectorWritable;
import com.yourorg.lob.model.SnapshotWritable;
import com.yourorg.lob.model.StockTimeKey;
import com.yourorg.lob.mr.job1.FactorPerStockMapper;
import com.yourorg.lob.mr.job1.FactorPerStockReducer;
import com.yourorg.lob.mr.job1.StockGroupingComparator;
import com.yourorg.lob.mr.job1.StockPartitioner;
import com.yourorg.lob.mr.job2.CrossSectionAvgCombiner;
import com.yourorg.lob.mr.job2.CrossSectionAvgMapper;
import com.yourorg.lob.mr.job2.CrossSectionAvgReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.File;

public class AppDriver extends Configured implements Tool {

    private void setupQuietLog() {
        try {
            Logger.getRootLogger().setLevel(Level.WARN);
        } catch (Throwable t) {
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        setupQuietLog();
        if (args.length < 3) return 2;

        Configuration conf = getConf();

        // 1. 默认文件系统设为 HDFS
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        // 2. 保持极速 Local 计算模式
        conf.set("mapreduce.framework.name", "local");

        // 3. 并发设置
        conf.setInt("mapreduce.local.map.tasks.maximum", 8);
        conf.setInt("mapreduce.local.reduce.tasks.maximum", 8);

        // 路径处理
        Path input = new Path(args[0]);
        // 强制加上 file:// 前缀，输出到本地
        String localTmpStr = args[1].startsWith("file:") ? args[1] : "file://" + args[1];
        String localOutStr = args[2].startsWith("file:") ? args[2] : "file://" + args[2];
        Path tmp = new Path(localTmpStr);
        Path output = new Path(localOutStr);

        FileSystem localFs = FileSystem.getLocal(conf);
        if (localFs.exists(tmp)) localFs.delete(tmp, true);
        if (localFs.exists(output)) localFs.delete(output, true);

        // ---------------- Job 1 ----------------
        Job job1 = Job.getInstance(conf, "lob-job1-per-stock");
        job1.setJarByClass(AppDriver.class);
        Configuration j1Conf = job1.getConfiguration();

        // 性能参数
        j1Conf.setInt("mapreduce.task.io.sort.mb", 100);
        j1Conf.setBoolean("mapreduce.map.output.compress", false);
        j1Conf.setInt("io.file.buffer.size", 131072);

        // 消除 EBADF
        j1Conf.setBoolean("mapreduce.ifile.readahead", false);

        // JIT 优化
        String fastJvm = "-Xmx1024m -XX:+UseParallelGC -XX:-UsePerfData -XX:CompileThreshold=1500 -XX:+TieredCompilation";
        j1Conf.set("mapreduce.map.java.opts", fastJvm);
        j1Conf.set("mapreduce.reduce.java.opts", fastJvm);

        job1.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job1, 41943040); // 40MB

        FileInputFormat.addInputPath(job1, input);
        job1.setMapperClass(FactorPerStockMapper.class);
        job1.setMapOutputKeyClass(StockTimeKey.class);
        job1.setMapOutputValueClass(SnapshotWritable.class);
        job1.setPartitionerClass(StockPartitioner.class);
        job1.setGroupingComparatorClass(StockGroupingComparator.class);
        job1.setReducerClass(FactorPerStockReducer.class);
        job1.setNumReduceTasks(4);

        job1.setOutputKeyClass(DayTimeKey.class);
        job1.setOutputValueClass(FactorVectorWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, tmp);

        if (!job1.waitForCompletion(true)) return 1;

        // ---------------- Job 2 ----------------
        Job job2 = Job.getInstance(conf, "lob-job2-cross-section-avg");
        job2.setJarByClass(AppDriver.class);

        job2.getConfiguration().set("mapreduce.map.java.opts", fastJvm);
        job2.getConfiguration().set("mapreduce.reduce.java.opts", fastJvm);

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, tmp);
        job2.setMapperClass(CrossSectionAvgMapper.class);
        job2.setMapOutputKeyClass(DayTimeKey.class);
        job2.setMapOutputValueClass(FactorVectorWritable.class);
        job2.setCombinerClass(CrossSectionAvgCombiner.class);
        job2.setReducerClass(CrossSectionAvgReducer.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job2, "csv", TextOutputFormat.class, NullWritable.class, Text.class);
        FileOutputFormat.setOutputPath(job2, output);

        boolean ok2 = job2.waitForCompletion(true);
        if (!ok2) return 3;

        fixJob2OutputFiles(localFs, output);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AppDriver(), args));
    }

    private static void fixJob2OutputFiles(FileSystem fs, Path outputDir) throws Exception {
        File localDir = new File(outputDir.toUri().getPath());

        if (!localDir.exists() || !localDir.isDirectory()) return;

        File[] files = localDir.listFiles();
        if (files == null) return;

        for (File f : files) {
            String name = f.getName();

            // 1. 清理规则：
            // - 以 "." 开头的文件 (比如 .0102.csv.crc)
            // - 以 ".crc" 结尾的文件
            // - _SUCCESS 标记文件
            if (name.startsWith(".") || name.endsWith(".crc") || name.equals("_SUCCESS")) {
                f.delete(); // 直接物理删除
                continue;
            }

            // 2. 清理空的 part- 文件
            if (f.isFile() && f.length() == 0 && name.startsWith("part-")) {
                f.delete();
                continue;
            }

            // 3. 重命名逻辑 (part-r-xxxxx -> 0102.csv)
            int idx = name.indexOf("-r-");
            if (idx > 0) {
                String newName = name.substring(0, idx); // 截取 0102.csv
                File target = new File(localDir, newName);

                // 如果目标文件已存在，先删掉旧的，防止冲突
                if (target.exists()) target.delete();

                // 重命名
                f.renameTo(target);
            }
        }
    }
}
