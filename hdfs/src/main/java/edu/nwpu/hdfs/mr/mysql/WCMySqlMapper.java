package edu.nwpu.hdfs.mr.mysql;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WCMySqlMapper
 */
public class WCMySqlMapper extends Mapper<LongWritable, MyDBWritable, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, MyDBWritable value, Context context) throws IOException, InterruptedException {
        System.out.println("key : " + key);
        String line = value.getTxt();
        System.out.println("id: " + value.getId() + ", name: " + value.getName() + ", txt: " + line);
        String[] arr = line.split(" ");
        for (String s :
                arr) {
            context.write(new Text(s), new IntWritable(1));
        }
    }
}
