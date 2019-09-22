package edu.nwpu.hdfs.maxtemp.allsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTempWCReducer extends Reducer<ComboKey, NullWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("MaxTempWCReducer.reduce");
        int year = key.getYear();
        int temp = key.getTemp();
        context.write(new IntWritable(year), new IntWritable(temp));
    }
}