package edu.nwpu.hdfs.mr.mapjoin.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CIDPartitioner extends Partitioner<ComboKey, NullWritable> {

    public int getPartition(ComboKey key, NullWritable nullWritable, int numPartitions) {
        return key.getCid() % numPartitions;
    }
}
