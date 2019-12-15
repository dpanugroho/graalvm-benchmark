package org.bdapro;

import org.bdapro.javastream.Person;
import org.bdapro.javastream.Streams;
import org.bdapro.others.Blender;
import org.bdapro.others.CountUppercase;
import org.bdapro.others.JavaScrabble;
import org.bdapro.tpch.ArrowTPCH;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;

import static org.bdapro.others.StandardDeviation.computeStandardDeviation;

@State(Scope.Benchmark)
public class BenchmarkExecutor {
    private static double[] values = new double[2000000];
    private static double[] numbers = new double[1000];
    private static Person[] people = Person.generatePeople(10000);

    {
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = i;
        }
    }

    @Benchmark
    public int scrabble() {
        return JavaScrabble.run();
    }

    @Benchmark
    public long countUppercase(){
        return CountUppercase.run();
    }

    @Benchmark
    public long blender() {
        return Blender.run();
    }

    @Benchmark
    public double standardDeviation() {
        return computeStandardDeviation(numbers);
    }

    @Benchmark
    public double mapReduce() {
        return Streams.mapReduce(values);
    }

    @Benchmark
    public double parMapReduce() {
        return Streams.parMapReduce(values);
    }

    @Benchmark
    public static double shortHairYoungsterHeight(){
        return Streams.shortHairYoungsterHeight(people);
    }

    @Benchmark
    public static double volleyballStars(){
        return Streams.volleyballStars(people);
    }
}
