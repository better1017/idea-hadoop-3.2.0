package edu.nwpu.hdfs.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public WCReducer() {
        System.out.println("new WCReducer()");
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Reducer输入的KV是Mapper输出的KV
        // 相同的key可能有多个迭代器的值，因此用Interable
        int count = 0;
        for (IntWritable iw :
                values) {
            count += iw.get();
        }
        String tno = Thread.currentThread().getName();
        System.out.println(tno + " : WCReducer : " + key.toString() + "=" + count);
        context.getCounter("r", Util.getInfo(this, "reduce")).increment(1);
        context.write(key, new IntWritable(count));
    }
}
