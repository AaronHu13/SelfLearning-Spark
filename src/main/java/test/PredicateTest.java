package test;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PredicateTest {
    public static void main(String[] args) {
//        Instant standard = Instant.ofEpochMilli(1401580800000l);
//        Instant t = Instant.ofEpochMilli(1401580700000l);
//        if(createdAfter(standard).test(t)){
//            System.out.println("After");
//        }
//        List<String> test = new LinkedList<>();
//        for(int i = 0; i < 10; i++){
//            test.add(String.valueOf(i));
//        }
//
//        System.out.println(test.stream().map(i -> new Tuple2<>(i, Integer.valueOf(i))));
//        System.out.println(getAlphaNumericString(10));
        test();
    }

    public static Predicate<Instant> createdAfter(Instant t)
    {
        return p -> {
            return p.isAfter(t);
        };
    }

    static String getAlphaNumericString(int n)
    {

        // chose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                + "0123456789"
                + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);

        for (int i = 0; i < n; i++) {

            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index
                    = (int)(AlphaNumericString.length()
                    * Math.random());

            // add Character one by one in end of sb
            sb.append(AlphaNumericString
                    .charAt(index));
        }

        return sb.toString();
    }

    public static void test(){
        List<String> l = new LinkedList<>();
        for(int i = 0; i < 10; i++) {
            l.add(String.valueOf(i));
        }

        l.add(String.valueOf(0));
        l.add(String.valueOf(1));
        l.add(String.valueOf(2));

        for(String s : l){
            System.out.println(s);
        }
        System.out.println("=========");
        List<String> strings = l.stream().map(t -> (t + "9")).collect(Collectors.toList());
//        Set<String> strings = l.stream().collect(Collectors.groupingBy(t -> t)).keySet();
        for(String s : strings){
            System.out.println(s);
        }

    }
}