package com.syyz.friends;

import org.apache.hadoop.io.*;

public class FriendGroup extends WritableComparator {

	protected FriendGroup() {
		super(User.class, true);
	}

	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {

		User p1 = (User) o1;
		User p2 = (User) o2;

		return p1.getName().compareTo(p2.getName());

	}
}
