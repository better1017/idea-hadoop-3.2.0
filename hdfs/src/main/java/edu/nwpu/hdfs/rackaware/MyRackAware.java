package edu.nwpu.hdfs.rackaware;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * 机架感知类
 */
public class MyRackAware implements DNSToSwitchMapping {

    public List<String> resolve(List<String> names) {
        List<String> list = new ArrayList<String>();
        for (String str : names) {
            // 输出原来的信息，IP地址(主机名)
            System.out.println(str);
            //
            if (str.startsWith("192")) {
                //192.168.11.135
                String ip = str.substring(str.lastIndexOf(".") + 1);
                if (Integer.parseInt(ip) <= 136) {
                    list.add("/rack1/" + ip);
                } else {
                    list.add("/rack2/" + ip);
                }
            }
            else if (str.startsWith("s")) {
                String ip =str.substring(1);
                if (Integer.parseInt(ip) <= 136) {
                    list.add("/rack1/" + ip);
                } else {
                    list.add("/rack2/" + ip);
                }
            }
        }

        return list;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> names) {

    }
}
