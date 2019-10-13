package edu.nwpu.hdfs.mr.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * join操作，map端连接
 */
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> allCustomers = new HashMap<String, String>();

    // 启动，初始化客户信息
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fis = fs.open(new Path("file:///D:/mr/mapjoin/customers.txt"));
            // 得到缓冲区阅读器
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String line = null;
            while ((line = br.readLine()) != null) {
                // 得到cid
                String cid = line.substring(0, line.indexOf(","));
                allCustomers.put(cid, line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 订单信息
        String line = value.toString();
        // 提取customer id
        String cid = line.substring(line.lastIndexOf(",") + 1);
        // 订单信息
        String orderInfo = line.substring(0, line.lastIndexOf(","));
        // 连接customer + "," + order
        String customerInfo = allCustomers.get(cid);
        context.write(new Text(customerInfo + "," + orderInfo), NullWritable.get());
    }
}