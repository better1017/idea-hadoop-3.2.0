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
public class WCChainMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable();
        String arr[] = value.toString().split(" ");
        for (String s : arr) {
            keyOut.set(s);
            valueOut.set(1);
            context.write(keyOut, valueOut);
        }
    }
}
