package edu.nwpu.hdfs.maxtemp.allsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ComboKeySortComparator extends WritableComparator {
    protected ComboKeySortComparator() {
        super(ComboKey.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        System.out.println("ComboKeySortComparator.compare");
        ComboKey k1 = (ComboKey) a;
        ComboKey k2 = (ComboKey) b;
        return k1.compareTo(k2);
    }
}
