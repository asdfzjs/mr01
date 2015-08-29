package com.syyz.friends;


import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public final class SortJobRun {
	
	public static void main(String[] args) {

		SortJobRun jr = new SortJobRun();
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
		 job.setJarByClass(SortJobRun.class);
		 job.setJobName("firends2");

		 job.setMapOutputKeyClass(User.class);
		 job.setMapOutputValueClass(User.class);
		 job.setSortComparatorClass(FriendsSort.class);
		 job.setGroupingComparatorClass(FriendGroup.class);
		// 默认进入MAP task 的记录格式
		// job.setInputFormatClass(TextInputFormat.class);
		// 进入MAP task 的记录格式：一条记录代表一行，一条记录中key和value是由一个分隔符隔开的。
		// 第一个分隔符左边是key，右边是value
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		// 指定MR的输入数据（文件）
		FileInputFormat.addInputPath(job, new Path("/usr/output/friends"));
		// 指定MR输出数据目录，该目录不能存在，MR在启动之处要检查该目录是否存在，如果存在报错。

		Path outpath = new Path("/usr/output/friends2");
		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		// 执行该任务（MR），并等待MR完成
		return job.waitForCompletion(true);

	}
  public static class Map
      extends Mapper<Text, Text, User, User> {

    private User outputKey = new User();
    private User outputValue = new User();

    @Override
    protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] parts = StringUtils.split(value.toString());
      String name = parts[0];
      int commonFriends = Integer.valueOf(parts[1]);

      outputKey.set(name, commonFriends);
      outputValue.set(key.toString(), commonFriends);
      context.write(outputKey, outputValue);

      outputValue.set(name, commonFriends);
      outputKey.set(key.toString(), commonFriends);
      context.write(outputKey, outputValue);
    }
  }

  public static class Reduce
      extends Reducer<User, User, Text, Text> {

    private Text name = new Text();
    private Text potentialFriends = new Text();

    @Override
    public void reduce(User key, Iterable<User> values,
                       Context context)
        throws IOException, InterruptedException {

      StringBuilder sb = new StringBuilder();

      int count = 0;
      for (User potentialFriend : values) {
        if(sb.length() > 0) {
          sb.append(",");
        }
        sb.append(potentialFriend.getName())
            .append(":")
            .append(potentialFriend.getCommonFriends());

        if (++count == 10) {
          break;
        }
      }

      name.set(key.getName());
      potentialFriends.set(sb.toString());
      context.write(name, potentialFriends);
    }
  }
}
