package com.syyz.mr02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator{

	public MySort() {
		super(MyKey.class,true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey o1 =(MyKey) a;
		MyKey o2 =(MyKey) b;
		int v =Integer.compare(o1.getYear(), o2.getYear());
		if(v==0){
			int v2= Integer.compare(o1.getMonth(), o2.getMonth());
			if(v2==0)
				return -Double.compare(o1.getHot(), o2.getHot());
			else
				return v2;
				
		}
		return v;
	}
}
