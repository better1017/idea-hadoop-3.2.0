package edu.nwpu.hdfs.mr.mapjoin.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * mapper
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text, ComboKey, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //一行数据
        String line = value.toString();
        //判断是customer还是order
        FileSplit split = (FileSplit) context.getInputSplit();
        String path = split.getPath().toString();

        ComboKey outKey = new ComboKey();
        //客户信息
        if (path.contains("customers")) {
            String cid = line.substring(0, line.indexOf(","));
            String custInfo = line;
            outKey.setType(0);//0表示客户
            outKey.setCid(Integer.parseInt(cid));
            outKey.setCustomerInfo(custInfo);
        } else { //订单信息
            String cid = line.substring(line.lastIndexOf(",") + 1);
            String oid = line.substring(0, line.indexOf(","));
            String orderInfo = line.substring(0, line.lastIndexOf(","));
            outKey.setType(1);//1表示订单
            outKey.setCid(Integer.parseInt(cid));
            outKey.setOid(Integer.parseInt(oid));
            outKey.setOrderInfo(orderInfo);
        }
        context.write(outKey, NullWritable.get());
    }
}
