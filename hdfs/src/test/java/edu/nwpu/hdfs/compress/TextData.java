package edu.nwpu.hdfs.compress;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class TextData {
    @Test
    public void maxTempTextData() throws IOException {
        /* 写入Txt文件 */
        File writename = new File("D:/mr/maxtemp/TextData/text.txt"); // 相对路径，如果没有则要建立一个新的output。txt文件
        writename.createNewFile(); // 创建新文件
        BufferedWriter out = new BufferedWriter(new FileWriter(writename));
//        out.write("我会写入文件啦\n"); // \r\n即为换行
        for (int i = 0; i < 6000; i++) {
            int year = 1970 + new Random().nextInt(100);
            int temp = -30 + new Random().nextInt(100);
            out.write(year + " " + temp + "\n");
        }
        out.flush(); // 把缓存区内容压入文件
        out.close(); // 最后记得关闭文件
    }
}
