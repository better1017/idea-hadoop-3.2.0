package edu.nwpu.hdfs.mr.chain;

import edu.nwpu.hdfs.mr.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yylin
 * @WCMapper
 */
public class WCChainMapper2 extends Mapper<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        if (!(key.toString().equals("falungong")
                || key.toString().equals("falundafahao")
                || key.toString().equals("isis"))) {
            context.write(key, value);
        }
    }
}
