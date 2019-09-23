package edu.nwpu.hdfs.mr.mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * WCMySqlAPP
 */
public class WCMySqlAPP {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        // 设置job的各种属性
        job.setJobName("单词计数-MySqlDB"); // 作业名称
        job.setJarByClass(WCMySqlAPP.class); // 搜索类

        // 设置数据库信息
        String driverClass = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/big4?serverTimezone=UTC";
        String userName = "root";
        String pwd = "123654";
        // 设置数据库配置
        DBConfiguration.configureDB(job.getConfiguration(), driverClass, url, userName, pwd);

        String inputQuery = "select id, name, txt from words";
        String inputCountQuery = "select count(*) from words";
        // 设置数据库输入
        DBInputFormat.setInput(job, MyDBWritable.class, inputQuery, inputCountQuery);

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\mr\\mr\\mysql\\out"));

        job.setMapperClass(WCMySqlMapper.class); // mapper类
        job.setReducerClass(WCMySqlReducer.class); // reducer类

        job.setNumReduceTasks(1); // reduce个数

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class); //输出KV的key类
        job.setOutputValueClass(IntWritable.class); // 输出KV的value类

        job.waitForCompletion(true);
    }
}
