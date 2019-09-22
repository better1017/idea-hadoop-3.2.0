package edu.nwpu.hdfs.mr.chain;

import edu.nwpu.hdfs.mr.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCChainReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Reducer输入的KV是Mapper输出的KV
        // 相同的key可能有多个迭代器的值，因此用Interable
        int count = 0;
        for (IntWritable iw : values) {
            count += iw.get();
        }
        String tno = Thread.currentThread().getName();
        context.write(key, new IntWritable(count));
    }
}
