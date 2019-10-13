package edu.nwpu.hdfs.mr.mapjoin.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 组合key排序对比器
 */
public class ComboKeyComparator extends WritableComparator {
    public ComboKeyComparator() {
        super(ComboKey.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        ComboKey k1 = (ComboKey) a;
        ComboKey k2 = (ComboKey) b;
        return k1.compareTo(k2);
    }
}
