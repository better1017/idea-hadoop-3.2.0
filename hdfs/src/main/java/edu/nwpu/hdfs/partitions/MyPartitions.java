package edu.nwpu.hdfs.partitions;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 */
public class MyPartitions extends Partitioner<Text, IntWritable> {
    public MyPartitions(){
        System.out.println("new MyPartitions()");
    }
    public int getPartition(Text text, IntWritable intWritable, int i) {
        System.out.println("MyPartitions().getPartition()");
        return 0;
    }
}
