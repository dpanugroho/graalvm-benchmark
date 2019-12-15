package org.bdapro.tpch;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.Schema;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ArrowTPCH {

    @State(Scope.Benchmark)
    public static class MySate{
        private List<ArrowBlock> arrowBlocks;
        private ArrowFileReader arrowFileReader;
        private VectorSchemaRoot root;

        @Param({"lineitem10.arrow","lineitem30.arrow","lineitem100.arrow","lineitem300.arrow","lineitem1k.arrow","lineitem3k.arrow","lineitem10k.arrow","lineitem30k.arrow"})
        public String lineItemFilePath;

        @Setup(Level.Trial)
        public void setup() throws IOException {
            File arrowFile = new File(lineItemFilePath);
            FileInputStream fileInputStream = new FileInputStream(arrowFile);
            arrowFileReader = new ArrowFileReader(new SeekableReadChannel(fileInputStream.getChannel()),
                    new RootAllocator(Integer.MAX_VALUE));
            root = arrowFileReader.getVectorSchemaRoot();
            Schema schema = root.getSchema();

            arrowBlocks = arrowFileReader.getRecordBlocks();
        }

        @TearDown(Level.Trial)
        public void doTearDown() throws IOException {
            arrowFileReader.close();
        }
    }

    @Benchmark @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public double executeQuerySix(MySate state) throws IOException {
        double totalRevenue = 0.0;
        for (ArrowBlock arrowBlock : state.arrowBlocks) {
            if (!state.arrowFileReader.loadRecordBatch(arrowBlock)) { // load the batch
                throw new IOException("Expected to read record batch");
            }
            // Get columns based on column order on TPC-H LineItem table
            BigIntVector lQuantity = (BigIntVector) state.root.getVector(4);
            Float8Vector lExtendedPrice = (Float8Vector) state.root.getVector(5);
            Float8Vector lDiscount = (Float8Vector) state.root.getVector(6);
            TimeStampSecVector lShipDate = (TimeStampSecVector) state.root.getVector(10);

            //Todo: Check if lQuantity, lPrice, lDiscount, and lShidate have the same size
            double currentBlockRevenue = 0.0;
            for (int i = 0; i < lQuantity.getValueCount(); i++) {
                if((lShipDate.get(i)>=757382401) // 1 jan 1994 = 757382401
                        && (lShipDate.get(i)<788918401) // 1 jan 1995 = 788918401
                        && (lDiscount.get(i) > 0.05) // 0.06 - 0.01
                        && (lDiscount.get(i)<0.07) // 0.06 + 0.07
                        && (lQuantity.get(i) > 24)){
                    currentBlockRevenue += lExtendedPrice.get(i) * lDiscount.get(i);
                }
            }
            totalRevenue += currentBlockRevenue;
        }
        return totalRevenue;
    }
}

