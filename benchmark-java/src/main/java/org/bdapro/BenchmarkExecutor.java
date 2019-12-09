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

    @Param({"lineitem10.arrow","lineitem30.arrow","lineitem100.arrow","lineitem300.arrow","lineitem1k.arrow","lineitem3k.arrow","lineitem10k.arrow","lineitem30k.arrow"})
    public String lineItemFilePath;

    @Benchmark @BenchmarkMode(Mode.Throughput)
    public double tpch6() {
        try {
            return ArrowTPCH.executeQuerySix(lineItemFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
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
