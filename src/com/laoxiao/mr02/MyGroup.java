package com.laoxiao.mr02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator{

	public MyGroup(){
		super(MyKey.class,true);
	}
	
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey o1 =(MyKey) a;
		MyKey o2 =(MyKey) b;
		int v =Integer.compare(o1.getYear(), o2.getYear());
		if(v==0){
			return Integer.compare(o1.getMonth(), o2.getMonth());
		}
		return v;
	}
}
