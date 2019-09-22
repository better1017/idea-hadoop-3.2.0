package edu.nwpu.hdfs.maxtemp.allsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<ComboKey, NullWritable> {
    public int getPartition(ComboKey key, NullWritable nullWritable, int numPartitions) {
        int year = key.getYear();
        System.out.println("YearPartitioner.getPartition : " + year);
        return year % numPartitions;
    }
}
