package com.laoxiao.mr02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable<MyKey>{

	private int year;
	private double hot;
	private int month;
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public double getHot() {
		return hot;
	}

	public void setHot(double hot) {
		this.hot = hot;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	/**
	 * 对象序列化
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeDouble(hot);
		out.writeInt(month);
	}

	/**
	 * 反序列化
	 */
	public void readFields(DataInput in) throws IOException {
		this.year =in.readInt();
		this.hot=in.readDouble();
		this.month=in.readInt();
	}

	/**
	 * 当该对象放入某些集合的时候
	 */
	public int compareTo(MyKey o) {
		int v1 =Integer.compare(this.year, o.getYear());
		if(v1==0){
			int v2 =Integer.compare(this.month, o.getMonth());
			if(v2==0){
				return Double.compare(this.hot, o.getHot());
			}else{
				return v2;
			}
		}else
			return v1;
	}
	

}
