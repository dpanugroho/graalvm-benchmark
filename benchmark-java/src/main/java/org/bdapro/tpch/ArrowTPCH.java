package org.bdapro.tpch;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@State(Scope.Benchmark)
public class ArrowTPCH {
    public static double executeQuerySix(String lineItemFilePath) throws IOException {
        File arrowFile = new File(lineItemFilePath);;
        FileInputStream fileInputStream = new FileInputStream(arrowFile);
        ArrowFileReader arrowFileReader = new ArrowFileReader(new SeekableReadChannel(fileInputStream.getChannel()),
                new RootAllocator(Integer.MAX_VALUE));
        VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();
        Schema schema = root.getSchema();

        List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();

        double totalRevenue = 0.0;
        for (ArrowBlock arrowBlock : arrowBlocks) {
            if (!arrowFileReader.loadRecordBatch(arrowBlock)) { // load the batch
                throw new IOException("Expected to read record batch");
            }
            // Get columns based on column order on TPC-H LineItem table
            BigIntVector lQuantity = (BigIntVector) root.getVector(4);
            Float8Vector lExtendedPrice = (Float8Vector) root.getVector(5);
            Float8Vector lDiscount = (Float8Vector) root.getVector(6);
            TimeStampSecVector lShipDate = (TimeStampSecVector) root.getVector(10);

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
        arrowFileReader.close();
        return totalRevenue;
    }
}

