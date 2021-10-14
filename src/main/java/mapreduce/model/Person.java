package mapreduce.model;

public class Person {


    private final int age;
    private final String name;

    public int getAge() {
       // System.out.print(Thread.currentThread().getName());
        return age;
    }

    public String getName() {
        return name;
    }

    public Person(int age, String name) {
        this.age = age;
        this.name = name;
    }
}
