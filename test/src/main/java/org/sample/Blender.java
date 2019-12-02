package org.sample;

public class Blender {

    private static class Color {
        double r, g, b;

        private Color(double r, double g, double b) {
            this.r = r;
            this.g = g;
            this.b = b;
        }

        public static Color black() {
            return new Color(0, 0, 0);
        }

        public void add(Color other) {
            r += other.r;
            g += other.g;
            b += other.b;
        }

        public void add(double nr, double ng, double nb) {
            r += nr;
            g += ng;
            b += nb;
        }

        public void multiply(double factor) {
            r *= factor;
            g *= factor;
            b *= factor;
        }
    }

    private static final Color[][][] colors = new Color[100][100][100];

    public static long run() {
        long total = 0;
        for (int j = 0; j < 10; j++) {
            long t = System.nanoTime();
            for (int i = 0; i < 100; i++) {
                initialize(new Color(j / 20, 0, 1));
            }
            long d = System.nanoTime() - t;
            total+= d;
        }
        return total;
    }

    private static void initialize(Color id) {
        for (int x = 0; x < colors.length; x++) {
            Color[][] plane = colors[x];
            for (int y = 0; y < plane.length; y++) {
                Color[] row = plane[y];
                for (int z = 0; z < row.length; z++) {
                    Color color = new Color(x, y, z);
                    color.add(id);
                    if ((color.r + color.g + color.b) % 42 == 0) {
                        // PEA only allocates a color object here.
                        row[z] = color;
                    } else {
                        // In this branch the color object is not allocated at all.
                    }
                }
            }
        }
    }
}
