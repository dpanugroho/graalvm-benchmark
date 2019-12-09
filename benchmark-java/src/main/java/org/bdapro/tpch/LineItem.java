package org.bdapro.tpch;

import java.util.Date;

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
