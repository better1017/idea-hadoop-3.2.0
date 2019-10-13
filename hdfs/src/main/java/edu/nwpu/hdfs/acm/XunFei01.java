package edu.nwpu.hdfs.acm;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class XunFei01 {
    public static void main(String[] args) {
        System.out.println(fun());
    }

    public static String fun() {
        Scanner sc = new Scanner(System.in);
        List<String> name = new ArrayList<String>();
        name.add("zhangsan");
        name.add("lisi");
        String testName = sc.next();
        for (String str : name) {
            if (testName.equals(str)) {
                return "该用户名已存在";
            }
        }
        String testPWD = sc.next();
        String regex1 = "^(?![0-9]+$)(?![a-zA-Z]+$)[0-9A-Za-z]{8,16}$";
        if (!testPWD.matches(regex1)) {
            return "密码格式错误";
        }
        String regex2 = "^[1][0-9]{10}$";
        String testNum = sc.next();
        if (testNum.matches(regex2)) {
            return "注册成功";
        } else {
            return "请输入正确的手机号码";
        }
    }
}
