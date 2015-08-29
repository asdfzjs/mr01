package com.laoxiao.mr02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<MyKey, Text>{
	/**
	 * 快速分区
	 */
	public int getPartition(MyKey key, Text value, int reduceNum) {

		return (key.getYear() & Integer.MAX_VALUE) % reduceNum;
	}

}
