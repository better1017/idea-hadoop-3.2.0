package edu.nwpu.hdfs.compress.testseqfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

public class TestSeqFile {
    /**
     * 写操作
     */
    @Test
    public void save() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("d:/mr/maxtemp/SeqData/temp.seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, IntWritable.class);
        for (int i = 0; i < 6000; i++) {
            int year = 1970 + new Random().nextInt(100);
            int temp = -30 + new Random().nextInt(100);
            writer.append(new IntWritable(year), new IntWritable(temp));
        }
        writer.close();
    }

    /**
     * 读操作, 循环输出所有的key-value
     */
    @Test
    public void read() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("d:/mr/mr/out/part-r-00000");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable value = new IntWritable();
        Text key = new Text();
        while (reader.next(key, value)) {
            System.out.println(key.toString() + " : " + value.get());
        }
        reader.close();
    }

    /**
     * 读操作, 得到当前value
     */
    @Test
    public void read2() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("d:/mr/seq_file/1.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key)) {
            reader.getCurrentValue(value);
            System.out.println(value.toString());
        }
        reader.close();
    }

    /**
     * 读操作, 得到当前value
     */
    @Test
    public void read3() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("d:/mr/seq_file/1.seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        // 得到文件指针
        long pos = reader.getPosition();
        System.out.println(pos);
        IntWritable key = new IntWritable();
        Text value = new Text();
        while (reader.next(key, value)) {
            System.out.println(reader.getPosition() + "   " + key.get() + " - " + value.toString());
        }
        reader.close();
    }
}
