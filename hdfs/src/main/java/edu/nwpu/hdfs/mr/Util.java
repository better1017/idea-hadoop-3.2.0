package edu.nwpu.hdfs.mr;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Util {

    public static String getInfo(Object o, String msg) {
        return getHostname() + ": " + getPID() + ": " + getTID() + ": " + getObjInfo(o) + ": " + msg;
    }

    /**
     * 得到主机名
     */
    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获得当前程序所在进程id
     */
    public static int getPID() {
        try {
            String info = ManagementFactory.getRuntimeMXBean().getName();
            return Integer.parseInt(info.substring(0, info.indexOf("@")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * 获得当前线程id
     */
    public static String getTID() {
        try {
            return Thread.currentThread().getName();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获得OID
     */
    public static String getObjInfo(Object o) {
        try {
            String sName = o.getClass().getSimpleName();
            return sName + "@" + o.hashCode();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
