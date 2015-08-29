package com.laoxiao.mr02;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRun {

	public static void main(String[] args) {

		JobRun jr =new JobRun();
		try {
			System.out.println(jr.run() ?"执行成功":"执行失败");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 启动MR程序
	 */
	public  boolean  run ()throws Exception{
		Configuration config = new  Configuration();
		config.set("fs.defaultFS", "hdfs://node7:8020");
		config.set("yarn.resourcemanager.hostname", "node7");
//		config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
		FileSystem fs =FileSystem.get(config);
		
		Job job =Job.getInstance(config);
//		job.setJarByClass(JobRun.class);
//		job.setJobName("hot");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setSortComparatorClass(MySort.class);
		job.setGroupingComparatorClass(MyGroup.class);
		
		job.setMapOutputKeyClass(MyKey.class);
		job.setMapOutputValueClass(Text.class);
		//设置reducer 任务的个数
		job.setNumReduceTasks(65);
		
		//指定MR的输入数据（文件）
		FileInputFormat.addInputPath(job, new Path("/usr/input/hot"));
		//指定MR输出数据目录，该目录不能存在，MR在启动之处要检查该目录是否存在，如果存在报错。
		
		Path outpath =new Path("/usr/output/hot");
		if(fs.exists(outpath)){
			fs.delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		//执行该任务（MR），并等待MR完成
		return job.waitForCompletion(true);
		
	}
	
  static	class MyMapper extends Mapper<LongWritable, Text, MyKey, Text>{
		 SimpleDateFormat SDF=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		 
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line =value.toString();
			//从这行数据中获取年份，月份，温度
			String[] strs =line.split("\t");
			try {
				Date date =SDF.parse(strs[0]);
				Calendar c  =Calendar.getInstance();
				c.setTime(date);
				int year =c.get(Calendar.YEAR);
				int month =c.get(Calendar.MONTH);
				if(month==0){
					System.out.println(line);
				}
				double hot =Double.parseDouble( strs[1].substring(0,strs[1].lastIndexOf("c")));
				MyKey outkey =new MyKey();
				outkey.setHot(hot);
				outkey.setYear(year);
				outkey.setMonth(month);
				context.write(outkey, value);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	static class MyReducer extends Reducer<MyKey, Text, NullWritable, Text>{
		NullWritable outkey =NullWritable.get();
		protected void reduce(MyKey key, Iterable<Text> iterable,
				Context context)
				throws IOException, InterruptedException {
			int i=0;
			System.out.println(key.getYear()+"*****"+key.getMonth());
			for(Text value :iterable){
				if(i==10){
					break;
				}
				i++;
				context.write(outkey, value);
			}
				
		}
	}
}
