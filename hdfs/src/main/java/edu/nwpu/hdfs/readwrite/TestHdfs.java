package edu.nwpu.hdfs.readwrite;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * 测试hdfs
 */
public class TestHdfs {

    @Test
    public void testRead() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream fis = fs.open(new Path("hdfs://s135:9000/user/yylin/hadoop/hello.txt"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(fis, baos, 1024);
        IOUtils.closeStream(fis);
        System.out.println(new String(baos.toByteArray()));
    }

    @Test
    public void testWrite() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream fos = fs.create(new Path("hdfs://s135:9000/user/yylin/hadoop1/a.txt"));
        fos.write("ni shi ge sha dan er?".getBytes());
        IOUtils.closeStream(fos);
    }

    @Test
    public void fun() throws IOException {
        Integer i1 = -129;
        Integer i2 = -129;
        Integer i3 = 127;
        Integer i4 = 127;
        System.out.println(i1==i2);
        System.out.println(i3==i4);
    }
}

