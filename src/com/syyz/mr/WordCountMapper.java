package com.syyz.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * map task输入数据为split内容，逐行输入map task。
 * LongWritable ：改行数所在文件中下标 
 * Text         ：改行数据
 * map输出数据：
 * 	word 1
 *  word 1
 *  word 1
 *  word 1
 *  
 *  word 4
 * @author root
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	Text outkey =new Text();
	IntWritable outvalue =new IntWritable();
	//map 方法会被循环调用。每行调用一次
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line =value.toString();
		System.out.println(line+"++++++++++++");
		//从每行数据中获取每个单词
		StringTokenizer st =new StringTokenizer(line);
		while(st.hasMoreTokens() ){
			String word =st.nextToken();
			outkey.set(word);
			outvalue.set(1);
			context.write(outkey, outvalue);
		}
	}
}
