package edu.nwpu.hdfs.mr.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class WCChainAPP {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        // 设置job的各种属性
        job.setJobName("单词计数"); // 作业名称
        job.setJarByClass(WCChainAPP.class); // 搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式(默认TextInputFormat文本模式)

        // 添加输入路径
        FileInputFormat.addInputPath(job, new Path("D:\\mr\\mr\\chain"));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\mr\\mr\\chain\\out"));

        // 在mapper链条上增加Mapper1
        ChainMapper.addMapper(job, WCChainMapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, conf);
        // 在mapper链条上增加Mapper2
        ChainMapper.addMapper(job, WCChainMapper2.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        // 在reducer链条上设置reducer
        ChainReducer.setReducer(job, WCChainReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);
        // 在reducer链条上设置mapper
        ChainReducer.addMapper(job, WCChainReduceMapper1.class, Text.class, IntWritable.class, Text.class, IntWritable.class, conf);

        job.setNumReduceTasks(1); // reduce个数

        job.waitForCompletion(true);
    }
}
