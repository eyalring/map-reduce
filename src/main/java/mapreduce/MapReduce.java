package mapreduce;

import mapreduce.model.Person;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MapReduce {
    public static void main(String[] args) {

        List<Person> personsList = createPersons(100000000);

        System.out.println("sequential start :");
        long startTime = System.currentTimeMillis();
        double sequentialResult = calculateAvergaeSequentialy(personsList);
        long endtime = System.currentTimeMillis();
        System.out.println("sequential ends ,result is " + sequentialResult + " time it took :" + (endtime - startTime));

        System.out.println("Stream start :");
        startTime = System.currentTimeMillis();
        double streamResult = calculateAvergaeUsingStreams(personsList);
        endtime = System.currentTimeMillis();
        System.out.println("Stream ends,result is " + streamResult + " time it took :" + (endtime - startTime));

        System.out.println("Parallel Stream start:");
        startTime = System.currentTimeMillis();
        double parallelStreamResult = calculateAvergaeUsingParallelStreams(personsList);
        endtime = System.currentTimeMillis();
        System.out.println("ParallelStream ends,result is " + parallelStreamResult + " time it took :" + (endtime - startTime));


    }

    private static double calculateAvergaeUsingParallelStreams(List<Person> personsList) {
        return personsList.parallelStream().mapToInt(p -> p.getAge()).average().getAsDouble();
    }

    private static double calculateAvergaeUsingStreams(List<Person> personsList) {
        return personsList.stream().mapToInt(p -> p.getAge()).average().getAsDouble();
    }

    private static double calculateAvergaeSequentialy(List<Person> personsList) {
        double sum = 0;
        for (Person person : personsList) {
            sum += new Double(person.getAge());
        }
        return sum / new Double(personsList.size());
    }

    private static List<Person> createPersons(int numberOfPersons) {
        List<Person> personList = new ArrayList<>();
        for (int i = 0; i < numberOfPersons; i++) {
            int age = new Random().nextInt(50);
            StringBuilder name = new StringBuilder("eyal");
            personList.add(new Person(age, name.append(age).toString()));
        }
        return personList;

    }
}