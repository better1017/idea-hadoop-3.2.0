package edu.nwpu.hdfs.maxtemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempWCAPP {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        // 设置job的各种属性
        job.setJobName("统计年度最高气温"); // 作业名称
        job.setJarByClass(MaxTempWCAPP.class); // 搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式(默认TextInputFormat文本模式)

        // 添加输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置分区类
        job.setPartitionerClass(YearPartition.class);

        job.setMapperClass(MaxTempWCMapper.class); // mapper类
        job.setReducerClass(MaxTempWCReducer.class); // reducer类

        job.setNumReduceTasks(3); // reduce个数

        /**
         * 设置Map的输出键值类
         */
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        /**
         * 设置最后的输出键值类
         */
        job.setOutputKeyClass(Text.class); //输出KV的key类
        job.setOutputValueClass(IntWritable.class); // 输出KV的value类

        job.waitForCompletion(true);
    }
}
