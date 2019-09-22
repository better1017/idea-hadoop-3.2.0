package edu.nwpu.hdfs.maxtemp.allsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTempWCAPP {
    public static void main(String[] args) throws Exception {
        System.out.println("MaxTempWCAPP.main");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        // 设置job的各种属性
        job.setJobName("统计年度最高气温-SecondarySort"); // 作业名称
        job.setJarByClass(MaxTempWCAPP.class); // 搜索类
        job.setInputFormatClass(TextInputFormat.class); //设置输入格式(默认TextInputFormat文本模式)

        // 添加输入路径
        FileInputFormat.addInputPath(job, new Path("D:\\mr\\maxtemp\\TextData\\text.txt"));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\mr\\maxtemp\\TextData\\out"));

        job.setMapperClass(MaxTempWCMapper.class); // mapper类
        job.setReducerClass(MaxTempWCReducer.class); // reducer类

        /**
         * 设置Map的输出键值类
         */
        job.setMapOutputKeyClass(ComboKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        /**
         * 设置最后的输出键值类
         */
        job.setOutputKeyClass(IntWritable.class); //输出KV的key类
        job.setOutputValueClass(IntWritable.class); // 输出KV的value类

        // 设置分区类
        job.setPartitionerClass(YearPartitioner.class);
        // 设置排序对比器
        job.setSortComparatorClass(ComboKeySortComparator.class);
        // 设置分组对比器
        job.setGroupingComparatorClass(YearGroupComparator.class);
        // reduce个数
            // 会根据我们重定义的分区方式进行将数据进行分区
            // 在每个分区上做mapreduce操作，
            // 即最终得到的结果在整体上也是有序的
        job.setNumReduceTasks(3);

        job.waitForCompletion(true);
    }
}