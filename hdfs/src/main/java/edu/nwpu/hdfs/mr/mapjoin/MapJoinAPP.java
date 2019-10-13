package edu.nwpu.hdfs.mr.mapjoin;

import edu.nwpu.hdfs.mr.WCMapper;
import edu.nwpu.hdfs.mr.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class MapJoinAPP {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        // 设置job的各种属性
        job.setJobName("MapJoinAPP"); // 作业名称
        job.setJarByClass(MapJoinAPP.class); // 搜索类

        // 添加输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(0); // 没有reduce

        job.setMapperClass(MapJoinMapper.class); // mapper类

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class); //输出KV的key类
        job.setOutputValueClass(IntWritable.class); // 输出KV的value类

        job.waitForCompletion(true);
    }
}
