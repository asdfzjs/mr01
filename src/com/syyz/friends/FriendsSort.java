package com.syyz.friends;

import org.apache.hadoop.io.*;

public class FriendsSort extends WritableComparator {
  	protected FriendsSort() {
		super(User.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		User p1 = (User) w1;
		User p2 = (User) w2;

		int cmp = p1.getName().compareTo(p2.getName());
		if (cmp != 0) {
			return cmp;
		}

		return -Long.compare(p1.getCommonFriends(), p2.getCommonFriends());
	}
}
