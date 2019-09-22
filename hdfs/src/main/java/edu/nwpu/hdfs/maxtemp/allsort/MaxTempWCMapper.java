package edu.nwpu.hdfs.maxtemp.allsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yylin
 * @WCMapper
 */
public class MaxTempWCMapper extends Mapper<LongWritable, Text, ComboKey, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] arr = line.split(" ");

        ComboKey comboKey = new ComboKey();
        comboKey.setYear(Integer.parseInt(arr[0]));
        comboKey.setTemp(Integer.parseInt(arr[1]));
        System.out.println("MaxTempWCMapper.map : " + arr[0] + " : " + arr[1]);
        context.write(comboKey, NullWritable.get());
    }
}
