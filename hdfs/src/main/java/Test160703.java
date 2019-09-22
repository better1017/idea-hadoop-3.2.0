interface A{
    public static final String MSG="hello";
    public abstract void printA();
}
interface B{
    public abstract void printB();
}
class C implements A,B{
    public void printA(){
        System.out.println("AAA");
    }

    public void printB() {
        System.out.println("BBB");
    }
}
public class Test160703 {
    public static void main(String[] args) {
        A a = new C();
        B b=(B)a;
        b.printB();
        System.out.println(a instanceof A);
        System.out.println(a instanceof B);
    }
}
