import java.util.Scanner;

public class Multiply {
    /*
       思路：乘法运算的时候每一个都要相乘也就是n*n
       当被乘数到达第二位的时候乘以乘数的第一位就要写到第二位开始由此得到
       结果摆放的位置按i+j
     */
    public String multiply(String num1, String num2) {
        StringBuilder res = new StringBuilder();
        char[] a = num1.toCharArray();
        char[] b = num2.toCharArray();
        if ((num1.length() == 1 && num1.equals("0")) || (num2.length() == 1 && num2.equals("0"))) return "0";
        //两数相乘最大不会超过两位相加的位数
        int[] result = new int[a.length + b.length];
        //两数倒向相乘
        for (int i = a.length - 1; i >= 0; i--) {
            for (int j = b.length - 1; j >= 0; j--) {
                result[a.length - 1 - i + b.length - 1 - j] += (a[i] - 48) * (b[j] - 48);
            }
        }
        for (int i = 0; i < result.length - 1; i++) {
            if (result[i] >= 10) {
                result[i + 1] += result[i] / 10;
                result[i] = result[i] % 10;
            }
        }
        //从前向后判断是否可以读取也就是第一位是不是为零
        boolean juge = false;
        for (int i = result.length - 1; i >= 0; i--) {
            if (result[i] != 0) {
                juge = true;
            }
            if (juge) {
                res.append(result[i]);
            }
        }
        return res.toString();
    }

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String a = sc.next();
        String b = sc.next();
        Multiply m = new Multiply();
        System.out.println(m.multiply(a, b));
    }
}
