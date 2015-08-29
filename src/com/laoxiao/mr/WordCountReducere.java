package com.laoxiao.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * reducer task : 输入数据 就是 map task的输出数据
 * @author root
 *
 */
public class WordCountReducere extends Reducer<Text, IntWritable, Text, IntWritable>{

	/**
	 * 每组数据传给该方法，key  （一个）表示一个单词，value （多个）
	 */
	protected void reduce(Text key, Iterable<IntWritable> iterable,
			Context context)
			throws IOException, InterruptedException {
		
		int sum =0;
		for( IntWritable i :iterable ){
			sum =sum+i.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
