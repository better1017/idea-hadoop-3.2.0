package edu.nwpu.hdfs.compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

public class TestMapFile {
    /**
     * 写文件MapFile
     */
    @Test
    public void write() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);
        MapFile.Writer writer = new MapFile.Writer(conf, fs, "d:/mr/map_file", IntWritable.class, Text.class);
        writer.append(new IntWritable(1), new Text("tom" + 1));
        writer.append(new IntWritable(2), new Text("tom" + 2));
        writer.close();
    }

    /**
     * 读文件MapFile
     */
    @Test
    public void read() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);
        MapFile.Reader reader = new MapFile.Reader(fs, "d:/mr/map_file", conf);
        IntWritable key = new IntWritable();
        Text value = new Text();
        System.out.println("*****************");
        while (reader.next(key, value)) {
            System.out.println(key.get() + " : " + value.toString());
        }
        System.out.println("*****************");
        reader.close();
        System.out.println("*****************");
    }
}
