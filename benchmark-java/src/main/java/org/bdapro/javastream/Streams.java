package org.bdapro.javastream;

import java.util.Arrays;

import org.bdapro.others.Blender;
import org.bdapro.others.CountUppercase;
import org.bdapro.others.JavaScrabble;
import org.bdapro.tpch.ArrowTPCH;
import org.openjdk.jmh.annotations.*;

public class Streams {


  public static double mapReduce(double[] values) {
    return Arrays.stream(values)
      .map(x -> x + 1)
      .map(x -> x * 2)
      .map(x -> x + 5)
      .reduce(0, Double::sum);
  }

  public static double parMapReduce(double[] values) {
    return Arrays.stream(values).parallel()
      .map(x -> x + 1)
      .map(x -> x * 2)
      .map(x -> x + 5)
      .reduce(0, Double::sum);
  }

  public static double shortHairYoungsterHeight(Person[] people) {
    double averageAge = Arrays.stream(people)
      .filter(p -> p.getHair() == Hairstyle.SHORT)
      .mapToInt(p -> p.getAge())
      .average().getAsDouble();
    return Arrays.stream(people)
      .filter(p -> p.getHair() == Hairstyle.SHORT)
      .filter(p -> p.getAge() < averageAge)
      .mapToInt(Person::getHeight)
      .average()
      .orElse(0.0);
  }

  public static double volleyballStars(Person[] people) {
    return Arrays.stream(people)
      .map(p -> new Person(Hairstyle.LONG, p.getAge() + 1, p.getHeight()))
      .filter(p -> p.getHeight() > 198)
      .filter(p -> p.getAge() >= 18 && p.getAge() <= 21)
      .mapToInt(p -> p.getHeight())
      .average().getAsDouble();
  }

}


