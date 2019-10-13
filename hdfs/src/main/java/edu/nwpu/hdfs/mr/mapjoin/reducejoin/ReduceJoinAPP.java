package edu.nwpu.hdfs.mr.mapjoin.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoinAPP {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        // 设置job的各种属性
        job.setJobName("ReduceJoinAPP"); // 作业名称
        job.setJarByClass(ReduceJoinAPP.class); // 搜索类

        // 添加输入路径
        FileInputFormat.addInputPath(job, new Path("D:\\mr\\reducejoin"));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\mr\\reducejoin\\out"));

        job.setMapperClass(ReduceJoinMapper.class); // mapper类
        job.setReducerClass(ReduceJoinReducer.class); // reducer类

        /**
         * 设置Map的输出键值类
         */
        job.setMapOutputKeyClass(ComboKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        /**
         * 设置最后的输出键值类
         */
        job.setOutputKeyClass(Text.class); //输出KV的key类
        job.setOutputValueClass(NullWritable.class); // 输出KV的value类

        // 设置分区类
        job.setPartitionerClass(CIDPartitioner.class);
        // 设置分组对比器
        job.setGroupingComparatorClass(CIDGroupComparator.class);
        // 设置排序对比器
        job.setSortComparatorClass(ComboKeyComparator.class);

        // reduce个数
        // 会根据我们重定义的分区方式进行将数据进行分区
        // 在每个分区上做mapreduce操作，
        // 即最终得到的结果在整体上也是有序的
        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
}