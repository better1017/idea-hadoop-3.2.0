package edu.nwpu.hdfs.mr.chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * reducer链条的mapper
 */
public class WCChainReduceMapper1 extends Mapper<Text, IntWritable, Text, IntWritable> {
    /**
     * 过滤单词个数
     */
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if (value.get() > 5) {
            context.write(key, value);
        }
    }
}
