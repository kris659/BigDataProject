package com.example.bigdata;

import org.apache.hadoop.io.Writable;

import java.io.*;

public class SalesStats implements Writable {
    int totalQuantity;
    double totalPrice;
    int mobileQuantity;

    public SalesStats() {}

    public SalesStats(int totalQuantity, double totalPrice, int mobileQuantity) {
        this.totalQuantity = totalQuantity;
        this.totalPrice = totalPrice;
        this.mobileQuantity = mobileQuantity;
    }

    public void reset() {
        totalQuantity = 0;
        totalPrice = 0.0;
        mobileQuantity = 0;
    }

    public void addStats(SalesStats other) {
        totalQuantity += other.totalQuantity;
        totalPrice += other.totalPrice;
        mobileQuantity += other.mobileQuantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(totalQuantity);
        out.writeDouble(totalPrice);
        out.writeInt(mobileQuantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalQuantity = in.readInt();
        totalPrice = in.readDouble();
        mobileQuantity = in.readInt();
    }
}