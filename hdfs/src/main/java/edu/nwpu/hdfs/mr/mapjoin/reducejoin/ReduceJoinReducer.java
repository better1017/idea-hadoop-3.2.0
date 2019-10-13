package edu.nwpu.hdfs.mr.mapjoin.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * ReduceJoinReducer, reducer端连接实现
 */
public class ReduceJoinReducer extends Reducer<ComboKey, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(ComboKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> it = values.iterator();
        it.next();
        int type = key.getType();
        int cid = key.getCid();
        String custInfo = key.getCustomerInfo();
        while (it.hasNext()) {
            it.next();
            String orderInfo = key.getOrderInfo();
            context.write(new Text(custInfo + "," + orderInfo), NullWritable.get());
        }
    }
}
