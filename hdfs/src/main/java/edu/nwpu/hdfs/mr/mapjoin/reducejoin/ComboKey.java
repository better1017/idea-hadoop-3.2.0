package edu.nwpu.hdfs.mr.mapjoin.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class ComboKey implements WritableComparable<ComboKey> {
    // 0:customer 1:order
    private int type;
    private int cid;
    private int oid;
    private String customerInfo = "";
    private String orderInfo = "";

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public String getCustomerInfo() {
        return customerInfo;
    }

    public void setCustomerInfo(String customerInfo) {
        this.customerInfo = customerInfo;
    }

    public String getOrderInfo() {
        return orderInfo;
    }

    public void setOrderInfo(String orderInfo) {
        this.orderInfo = orderInfo;
    }

    public int compareTo(ComboKey o) {
        int type0 = o.type;
        int cid0 = o.cid;
        int oid0 = o.oid;
        String customerInfo0 = o.customerInfo;
        String orderInfo0 = o.orderInfo;
        // 判断是否是同一个customer的数据
        if (cid == cid0) {
            if (type == type0) {//同一个客户的两个订单
                return oid - oid0;//订单升序
//                return -(oid - oid0);//订单降序
            } else {//一个customer + 他的order
                if (type == 0) {//客户在前
                    return -1;
                } else {//订单在后
                    return 1;
                }
            }
        } else { //不是同一个客户
            return cid - cid0;//客户升序
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeInt(cid);
        out.writeInt(oid);
        out.writeUTF(customerInfo);
        out.writeUTF(orderInfo);
    }

    public void readFields(DataInput in) throws IOException {
        this.type = in.readInt();
        this.cid = in.readInt();
        this.oid = in.readInt();
        this.customerInfo = in.readUTF();
        this.orderInfo = in.readUTF();
    }
}
