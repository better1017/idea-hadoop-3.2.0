import java.util.Scanner;

public class Test1607 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int a[] = new int[5];
        int i = 0;
        int j = 0;
        do {
            a[i] = sc.nextInt();
            i++;
        } while ((a[i - 1] != -1) && (i < 5));
        for (j = 0; j < i - 1; j++) {
            System.out.println(a[j] + " ");
        }
    }
}
