package org.example.arrow.io;

import io.netty.util.internal.ThreadLocalRandom;

import static org.example.arrow.io.Util.pickRandom;

public class Person {
    private static final String[] FIRST_NAMES = new String[]{"John", "Jane", "Gerard", "Aubrey", "Amelia"};
    private static final String[] LAST_NAMES = new String[]{"Smith", "Parker", "Phillips", "Jones"};

    private final String firstName;
    private final String lastName;
    private final int age;
    private final Address address;

    static Person randomPerson() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return new Person(
                pickRandom(FIRST_NAMES),
                pickRandom(LAST_NAMES),
                random.nextInt(0, 120),
                Address.randomAddress()
        );
    }

    public Person(String firstName, String lastName, int age, Address address) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.address = address;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getAge() {
        return age;
    }


    public Address getAddress() {
        return address;
    }
}
