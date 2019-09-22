package edu.nwpu.hdfs.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTempWCReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemp = Integer.MIN_VALUE;
        for (IntWritable iw :
                values) {
            maxTemp = Math.max(iw.get(), maxTemp);
        }
        context.write(key, new IntWritable(maxTemp));
    }
}
