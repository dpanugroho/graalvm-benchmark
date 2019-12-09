package org.bdapro.javastream;

import java.util.Random;

public class Person {
  static final double LONG_RATIO = 0.4;
  static final int MAX_AGE = 100;
  static final int MAX_HEIGHT = 200;

  private Hairstyle hair;
  private int age;
  private int height;

  public Person(Hairstyle hair, int age, int height) {
    this.hair = hair;
    this.age = age;
    this.height = height;
  }

  public Hairstyle getHair() {
    return hair;
  }

  public int getAge() {
    return age;
  }

  public int getHeight() {
    return height;
  }

  public static Person[] generatePeople(int total) {
    Random random = new Random(10);
    Person[] people = new Person[total];
    for (int i = 0; i < total; i++) {
      people[i] = new Person(
        random.nextDouble() < LONG_RATIO ? Hairstyle.LONG : Hairstyle.SHORT,
        (int)(random.nextDouble() * MAX_AGE),
        (int)(random.nextDouble() * MAX_HEIGHT));
    }
    return people;
  }
}
