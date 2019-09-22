package edu.nwpu.hdfs.maxtemp.allsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义组合key
 */
public class ComboKey implements WritableComparable {
    private int year;
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    /**
     * 对key进行比较实现
     * 是一个聚合的过程
     * 两两key相比较，然后排序
     */
    public int compareTo(Object o) {
        System.out.println("ComboKey.compareTo");
        ComboKey c = (ComboKey) o;
        int y0 = c.getYear();
        int t0 = c.getTemp();
        // 年份相同
        if (year == y0) {
            // 气温降序
            return -(temp - t0);
        }
        // 年份不同
        else {
            // 年份升序
            return year - y0;
        }
    }

    /**
     * 串行化过程(序列化）
     */
    public void write(DataOutput out) throws IOException {
        // 年份
        out.writeInt(this.getYear());
        // 气温
        out.writeInt(this.getTemp());
    }

    /**
     * 反序列化
     */
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.temp = in.readInt();
    }
}
