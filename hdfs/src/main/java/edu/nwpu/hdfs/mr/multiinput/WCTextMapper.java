package edu.nwpu.hdfs.mr.multiinput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yylin
 * @WCMapper
 */
public class WCTextMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //输入的的KV:
            //key: 行偏移量 value: 一行文本
        //输出的KV：
            //key: 一个单词 value: 数值1
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
