import java.util.Scanner;

public class test092701 {
    public static void main(String[] args) {
        // 第一行输入name，第二行输入typed
        Scanner sc = new Scanner(System.in);
        char[] name = sc.next().toCharArray();
        char[] typed = sc.next().toCharArray();
        int i = 0;
        int j = 0;
        while (i < name.length && j < typed.length) {
            if (name[i] == typed[j]) { // 字符相等
                i++;
                j++;
            } else if (typed[j] == typed[j - 1]) { // 字符被长按
                j++;
            } else { // 混进了奇怪的东西
                break;
            }
        }
        j++;
        while (j < typed.length) { // 遍历完typed串
            if (typed[j] == typed[j - 1]) { // 保证剩下的是被长按产生的
                j++;
            } else {
                break;
            }
        }
        if (i == name.length && j == typed.length) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
    }
}
