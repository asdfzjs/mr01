package com.syyz.friends;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FirendsOfFrends {

	public static void main(String[] args) {

		FirendsOfFrends jr = new FirendsOfFrends();
		try {
			System.out.println(jr.run() ? "执行成功" : "执行失败");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 启动MR程序
	 */
	public boolean run() throws Exception {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node7:8020");
		config.set("yarn.resourcemanager.hostname", "node7");
		// config.set("mapred.jar",
		// "C:\\Users\\Administrator\\Desktop\\wc.jar");
		FileSystem fs = FileSystem.get(config);

		Job job = Job.getInstance(config);
		 job.setJarByClass(FirendsOfFrends.class);
		 job.setJobName("firends");

		 job.setMapOutputKeyClass(FOF.class);
		 job.setMapOutputValueClass(IntWritable.class);
		// 默认进入MAP task 的记录格式
		// job.setInputFormatClass(TextInputFormat.class);
		// 进入MAP task 的记录格式：一条记录代表一行，一条记录中key和value是由一个分隔符隔开的。
		// 第一个分隔符左边是key，右边是value
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReduce.class);
		
		// 指定MR的输入数据（文件）
		FileInputFormat.addInputPath(job, new Path("/usr/input/friends"));
		// 指定MR输出数据目录，该目录不能存在，MR在启动之处要检查该目录是否存在，如果存在报错。

		Path outpath = new Path("/usr/output/friends");
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		// 执行该任务（MR），并等待MR完成
		return job.waitForCompletion(true);

	}

	static class FriendsMapper extends Mapper<Text, Text, FOF, IntWritable> {
		FOF fof = new FOF();
		IntWritable onevalue = new IntWritable(1);//fof中的两个用户是直接朋友关系
		IntWritable twovalue = new IntWritable(2);//fof中的两个用户非朋友关系

		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] friends = StringUtils.split(value.toString());
			for (int i = 0; i < friends.length; i++) {
				fof.set(key.toString(), friends[i]);
				context.write(fof, onevalue);
				for (int j =i+1; j < friends.length; j++) {
					fof.set(friends[i], friends[j]);
					System.out.println(fof.toString());
					context.write(fof, twovalue);
				}
			}
		}
	}

	static class FriendsReduce extends
			Reducer<FOF, IntWritable, FOF, IntWritable> {

		private IntWritable friendsInCommon = new IntWritable();

		public void reduce(FOF key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			System.out.println(key+"********");
			int commonFriends = 0;
			boolean alreadyFriends = false;
			for (IntWritable hops : values) {
				if (hops.get() == 1) {
					alreadyFriends = true;
					break;
				}

				commonFriends++;
			}
			if (!alreadyFriends) {
				friendsInCommon.set(commonFriends);
				context.write(key, friendsInCommon);
			}
		}
	}
}
