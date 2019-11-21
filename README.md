# GraalVM Benchmark

## Build Project
To build, go to the `test` folder, and run `mvn package`.

## Build Native Image
```native-image -jar target/benchmark.jar```
## Run Benchmark
### Execute on JVM
```java -XX:-UseJVMCICompiler -jar target/benchmarks.jar <function-name> -f1 -wi 4 -i4```
### Execute on GraalVM
```java -jar target/benchmarks.jar <function-name> -f1 -wi 4 -i4```
### Execute on GraalVM Native Image
 ```./benchmark <function-name> -f1 -wi 4 -i4```
