package edu.nwpu.hdfs.mr.multiinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCAPP {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        // 设置job的各种属性
        job.setJobName("单词计数"); // 作业名称
        job.setJarByClass(WCAPP.class); // 搜索类

        // 多个输入
        MultipleInputs.addInputPath(job, new Path("file:///d:/mr/mr/txt"), TextInputFormat.class, WCTextMapper.class);
        MultipleInputs.addInputPath(job, new Path("file:///d:/mr/mr/seq_file"), SequenceFileInputFormat.class, WCSeqMapper.class);

        //设置输出
        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        job.setReducerClass(WCReducer.class); // reducer类
        job.setNumReduceTasks(1); // reduce个数

        job.setOutputKeyClass(Text.class); //输出KV的key类
        job.setOutputValueClass(IntWritable.class); // 输出KV的value类

        job.waitForCompletion(true);
    }
}
