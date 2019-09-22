package edu.nwpu.hdfs.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartition extends Partitioner<IntWritable, IntWritable> {

    @Override
    public int getPartition(IntWritable curYear, IntWritable temp, int numPartitions) {
        // 1900-1939 1940-1979 1980-2019 : 分3份，一份40年
        // 1900-2019
//        System.out.println("**************" + numPartitions + "*********************");
        int start = 1900; //开始年份
        int end = 2019; //结束年份
        int years = end - start + 1; //总共的年数
        int width = years / numPartitions; //每个任务的年度区间
        int res = (curYear.get() - start) / width; //当前年数的任务返回给哪个任务
        //System.out.println("*******************" + curYear.get() + "------" + res + "*************");
        return res;
    }
}
