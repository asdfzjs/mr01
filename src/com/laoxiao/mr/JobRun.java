package com.laoxiao.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
//		config.set("fs.defaultFS", "hdfs://node7:8020");
//		config.set("yarn.resourcemanager.hostname", "node7");
		config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
		FileSystem fs =FileSystem.get(config);
		
		Job job =Job.getInstance(config);
		job.setJarByClass(JobRun.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducere.class);
		
		//执行一个Combiner程序
		job.setCombinerClass(WordCountReducere.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		//指定MR的输入数据（文件）
		FileInputFormat.addInputPath(job, new Path("/usr/input/wc/test"));
		//指定MR输出数据目录，该目录不能存在，MR在启动之处要检查该目录是否存在，如果存在报错。
		
		Path outpath =new Path("/usr/output/wc");
		if(fs.exists(outpath)){
			fs.delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		//执行该任务（MR），并等待MR完成
		return job.waitForCompletion(true);
		
	}
}
