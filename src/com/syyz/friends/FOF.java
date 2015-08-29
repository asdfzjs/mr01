package com.syyz.friends;

import org.apache.hadoop.io.Text;

/**
 * 封装user的朋友的朋友
 * user1\tuser2
 * @author root
 *
 */
public class FOF extends Text{
	
	
	public FOF(){
		super();
	}
	
	public FOF(String user1,String user2){
		super(getFOF(user1,user2));
	}
	
	public void set(String user1,String user2){
		super.set(getFOF(user1, user2));
	}
	
	/**
	 * 把两个用户名，连接，为了保证顺序一致，使用自然排序两个用户名
	 * @param user1
	 * @param user2
	 * @return
	 */
	public static String getFOF(String user1,String user2) {
		if (user1.compareTo(user2) < 0) {
	        return user1 + "\t" + user2;
	      }
	      return user2 + "\t" + user1;
	}
}
