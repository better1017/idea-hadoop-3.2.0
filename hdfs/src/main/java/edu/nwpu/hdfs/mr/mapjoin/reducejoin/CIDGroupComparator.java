package edu.nwpu.hdfs.mr.mapjoin.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组对比器
 */
public class CIDGroupComparator extends WritableComparator {
    public CIDGroupComparator() {
        super(ComboKey.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey k1 = (ComboKey) a;
        ComboKey k2 = (ComboKey) b;
        return k1.getCid() - k2.getCid();
    }
}
