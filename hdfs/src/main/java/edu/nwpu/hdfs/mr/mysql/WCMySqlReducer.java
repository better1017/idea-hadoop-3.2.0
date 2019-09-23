package edu.nwpu.hdfs.mr.mysql;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCMySqlReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int cnt = 0;
        for (IntWritable iw :
                values) {
            cnt += iw.get();
        }
        context.write(key, new IntWritable(cnt));
    }
}
