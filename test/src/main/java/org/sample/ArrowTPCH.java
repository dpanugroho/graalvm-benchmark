package org.sample;

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

    public static double run() {
        ArrowTPCH reader = new ArrowTPCH();
        try {
            return reader.makeRead("lineitem.arrow");
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public double makeRead(String filename) throws IOException {
        File arrowFile = Utils.validateFile(filename, true);
        FileInputStream fileInputStream = new FileInputStream(arrowFile);
        ArrowFileReader arrowFileReader = new ArrowFileReader(new SeekableReadChannel(fileInputStream.getChannel()),
                new RootAllocator(Integer.MAX_VALUE));
        VectorSchemaRoot root = arrowFileReader.getVectorSchemaRoot();
        Schema schema = root.getSchema();

        List<ArrowBlock> arrowBlocks = arrowFileReader.getRecordBlocks();
        List<LineItem> allItems = new ArrayList<>();

        for (ArrowBlock arrowBlock : arrowBlocks) {
            List<FieldVector> fieldVectors = root.getFieldVectors();

            List<LineItem> items = new ArrayList<>();
            for (int j = 0; j < root.getRowCount(); j++) {
                items.add(new LineItem());
            }

            for (int j = 0; j < fieldVectors.size(); j++) {
                FieldVector fieldVector = fieldVectors.get(j);
                Types.MinorType type = fieldVector.getMinorType();
                switch (type) {
                    case BIGINT: {
                        List<Long> values = getBigIntValue(fieldVector);
                        if (j == 0) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlOrderKey(values.get(k));
                            }
                        } else if (j == 1) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlPartKey(values.get(k));
                            }
                        } else if (j == 2) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlSuppKey(values.get(k));
                            }
                        } else if (j == 3) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlLineNumber(values.get(k));
                            }
                        } else if (j == 4) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlQuantity(values.get(k));
                            }
                        }
                        break;
                    }
                    case FLOAT8: {
                        List<Double> values = getFloat8Value(fieldVector);
                        if (j == 5) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlExtendedPrice(values.get(k));
                            }
                        } else if (j == 6) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlDiscount(values.get(k));
                            }
                        } else if (j == 7) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlTax(values.get(k));
                            }
                        }
                        break;
                    }
                    case VARCHAR: {
                        List<String> values = getVarCharValue(fieldVector);
                        if (j == 8) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlReturnFlag(values.get(k));
                            }
                        } else if (j == 9) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlLineStatus(values.get(k));
                            }
                        } else if (j == 13) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlShipInstruct(values.get(k));
                            }
                        } else if (j == 14) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlShipMode(values.get(k));
                            }
                        } else if (j == 15) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlComment(values.get(k));
                            }
                        }
                        break;
                    }
                    case TIMESTAMPSEC: {
                        List<Date> values = getTimeStampSecValue(fieldVector);
                        if (j == 10) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlShipDate(values.get(k));
                            }
                        } else if (j == 11) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlCommitDate(values.get(k));
                            }
                        } else if (j == 12) {
                            for (int k = 0; k < values.size(); k++) {
                                items.get(k).setlReceiptDate(values.get(k));
                            }
                        }
                        break;
                    }
                    default:
                        break;
                }
            }

            allItems.addAll(items);
        }
        arrowFileReader.close();
        return Queries.tpch6(allItems);
    }

    private List<Long> getBigIntValue(FieldVector fieldVector) {
        List<Long> values = new ArrayList<>();
        BigIntVector bigIntVector = (BigIntVector) fieldVector;
        for (int i = 0; i < bigIntVector.getValueCount(); i++) {
            if (!bigIntVector.isNull(i)) {
                long value = bigIntVector.get(i);
                values.add(value);
            } else {
                values.add(null);
            }
        }
        return values;
    }

    private List<Double> getFloat8Value(FieldVector fieldVector) {
        List<Double> values = new ArrayList<>();
        Float8Vector float8Vector = (Float8Vector) fieldVector;
        for (int i = 0; i < float8Vector.getValueCount(); i++) {
            if (!float8Vector.isNull(i)) {
                double value = float8Vector.get(i);
                values.add(value);
            } else {
                values.add(null);
            }
        }
        return values;
    }

    private List<String> getVarCharValue(FieldVector fieldVector) {
        List<String> values = new ArrayList<>();
        VarCharVector varCharVector = (VarCharVector) fieldVector;
        for (int i = 0; i < varCharVector.getValueCount(); i++) {
            if (!varCharVector.isNull(i)) {
                byte[] value = varCharVector.get(i);
                values.add(Arrays.toString(value));
            } else {
                values.add(null);
            }
        }
        return values;
    }

    private List<Date> getTimeStampSecValue(FieldVector fieldVector) {
        List<Date> values = new ArrayList<>();
        TimeStampSecVector timeStampSecVector = (TimeStampSecVector) fieldVector;
        for (int i = 0; i < timeStampSecVector.getValueCount(); i++) {
            if (!timeStampSecVector.isNull(i)) {
                long value = timeStampSecVector.get(i);
                values.add(new Date(value * 1000));
            } else {
                values.add(null);
            }
        }
        return values;
    }

}

class Queries {
    public static double tpch6(List<LineItem> items) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = simpleDateFormat.parse("19940101");
            endDate = simpleDateFormat.parse("19950101");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Date finalStartDate = startDate;
        Date finalEndDate = endDate;
        return items.stream().filter(lineItem -> lineItem.getlShipDate().compareTo(finalStartDate) >= 0 &&
                lineItem.getlShipDate().compareTo(finalEndDate) < 0 && lineItem.getlDiscount() > (0.06 - 0.01) &&
                lineItem.getlDiscount() < (0.06 + 0.01) && lineItem.getlQuantity() < 24)
                .mapToDouble(value -> value.getlExtendedPrice() * value.getlDiscount()).sum();
    }
}

class LineItem {
    private long lOrderKey;
    private long lPartKey;
    private long lSuppKey;
    private long lLineNumber;
    private long lQuantity;
    private double lExtendedPrice;
    private double lDiscount;
    private double lTax;
    private String lReturnFlag;
    private String lLineStatus;
    private Date lShipDate;
    private Date lCommitDate;
    private Date lReceiptDate;
    private String lShipInstruct;
    private String lShipMode;
    private String lComment;

    public LineItem() {
    }

    public long getlOrderKey() {
        return lOrderKey;
    }

    public void setlOrderKey(long lOrderKey) {
        this.lOrderKey = lOrderKey;
    }

    public long getlPartKey() {
        return lPartKey;
    }

    public void setlPartKey(long lPartKey) {
        this.lPartKey = lPartKey;
    }

    public long getlSuppKey() {
        return lSuppKey;
    }

    public void setlSuppKey(long lSuppKey) {
        this.lSuppKey = lSuppKey;
    }

    public long getlLineNumber() {
        return lLineNumber;
    }

    public void setlLineNumber(long lLineNumber) {
        this.lLineNumber = lLineNumber;
    }

    public long getlQuantity() {
        return lQuantity;
    }

    public void setlQuantity(long lQuantity) {
        this.lQuantity = lQuantity;
    }

    public double getlExtendedPrice() {
        return lExtendedPrice;
    }

    public void setlExtendedPrice(double lExtendedPrice) {
        this.lExtendedPrice = lExtendedPrice;
    }

    public double getlDiscount() {
        return lDiscount;
    }

    public void setlDiscount(double lDiscount) {
        this.lDiscount = lDiscount;
    }

    public double getlTax() {
        return lTax;
    }

    public void setlTax(double lTax) {
        this.lTax = lTax;
    }

    public String getlReturnFlag() {
        return lReturnFlag;
    }

    public void setlReturnFlag(String lReturnFlag) {
        this.lReturnFlag = lReturnFlag;
    }

    public String getlLineStatus() {
        return lLineStatus;
    }

    public void setlLineStatus(String lLineStatus) {
        this.lLineStatus = lLineStatus;
    }

    public Date getlShipDate() {
        return lShipDate;
    }

    public void setlShipDate(Date lShipDate) {
        this.lShipDate = lShipDate;
    }

    public Date getlCommitDate() {
        return lCommitDate;
    }

    public void setlCommitDate(Date lCommitDate) {
        this.lCommitDate = lCommitDate;
    }

    public Date getlReceiptDate() {
        return lReceiptDate;
    }

    public void setlReceiptDate(Date lReceiptDate) {
        this.lReceiptDate = lReceiptDate;
    }

    public String getlShipInstruct() {
        return lShipInstruct;
    }

    public void setlShipInstruct(String lShipInstruct) {
        this.lShipInstruct = lShipInstruct;
    }

    public String getlShipMode() {
        return lShipMode;
    }

    public void setlShipMode(String lShipMode) {
        this.lShipMode = lShipMode;
    }

    public String getlComment() {
        return lComment;
    }

    public void setlComment(String lComment) {
        this.lComment = lComment;
    }
}

class Utils {
    static File validateFile(String fileName, boolean shouldExist) {
        if (fileName == null) {
            throw new IllegalArgumentException("Missing file parameter");
        }
        File file = new File(fileName);
        if (shouldExist && (!file.exists() || file.isDirectory())) {
            throw new IllegalArgumentException(fileName + " file not found: " + file.getAbsolutePath());
        }
        if (!shouldExist && file.exists()) {
            throw new IllegalArgumentException(fileName + " file already exists: " + file.getAbsolutePath());
        }
        return file;
    }
}