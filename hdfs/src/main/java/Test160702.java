import java.util.Scanner;

public class Test160702 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int a[] = new int[n];
        for (int i = 0; i <= n; i++) {
            a[i] = i;
            System.out.println(a[i]);
        }
    }
}
