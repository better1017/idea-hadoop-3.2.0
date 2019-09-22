package edu.nwpu.hdfs.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * 测试压缩
 */
public class TestCompression {
    @Test
    public void testCompress() throws Exception {
        /*String[] codecClassStrArr = {
                "DeflateCodec",
                "GzipCodec",
                "BZip2Codec",
                "Lz4Codec",
                "SnappyCodec"
        };*/
        Class[] codecClassStrArr = {
                DeflateCodec.class,
                GzipCodec.class,
                BZip2Codec.class,
                Lz4Codec.class
                //SnappyCodec.class
                //java.lang.RuntimeException: native snappy library not available: this version of libhadoop was built without snappy support.

        };
        for (Class str :
                codecClassStrArr) {
            ZipTest(str);
            unZipTest(str);
        }
    }

    public void ZipTest(Class codecClass) throws Exception {
        long start = System.currentTimeMillis();
        // 实例化对象
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());
        //创建文件输出流，得到默认扩展名
        FileOutputStream fos = new FileOutputStream("d:/mr/test" + codec.getDefaultExtension());
        //得到压缩输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);
        //压缩输出
        IOUtils.copyBytes(new FileInputStream("d:/mr/test.txt"), cos, 1024);
        //关闭输出流
        IOUtils.closeStream(cos);
        System.out.println(codecClass.getSimpleName() + ": " + (System.currentTimeMillis() - start));
    }

    public void unZipTest(Class codecClass) throws Exception {
        long start = System.currentTimeMillis();
        // 实例化对象
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());
        //创建文件输入流，得到默认扩展名
        FileInputStream fis = new FileInputStream("d:/mr/test" + codec.getDefaultExtension());
        //得到压缩输入流
        CompressionInputStream cis = codec.createInputStream(fis);
        //拷贝字节
        IOUtils.copyBytes(cis, new FileOutputStream("d:/mr/test" + codec.getDefaultExtension() + ".txt"), 1024);
        //关闭输出流
        IOUtils.closeStream(cis);
        System.out.println(codecClass.getSimpleName() + " : " + (System.currentTimeMillis() - start));
    }
}
