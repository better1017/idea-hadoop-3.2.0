public class test092702 {
    public static void main(String[] args) {
        System.out.println(B.c);
    }

    static class A {
        static {
            System.out.println("A");
        }
    }

    static class B extends A {
        static {
            System.out.println("B");
        }

        public final static String c = "C";
    }
}