package org.bdapro.others;

import java.util.Arrays;

public class StandardDeviation {
    public static double computeStandardDeviation(double[] numbers) {
        double total = Arrays.stream(numbers).sum();
        double mean = total / numbers.length;
        double stdev = Arrays.stream(numbers)
                .map(x -> x - mean)
                .map(d -> d * d).sum();
        return stdev;
    }
}
