package org.bdapro.others;

public class CountUppercase {
    public static long run() {
        String sentence = String.join(" ", "Lorem ipsum dolor sit amet, conSEctetur adipiscing eLit, sed DO eiusmod tempor incididunt ut LABORE et dolore magna aliqua.");
        long total = 0;
        for (int i = 1; i < 10_000_000; i++) {
            total += sentence.chars().filter(Character::isUpperCase).count();
        }
        return total;
    }
}