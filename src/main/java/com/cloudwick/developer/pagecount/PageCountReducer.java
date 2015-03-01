package com.cloudwick.developer.pagecount;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageCountReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		IntWritable temp = new IntWritable();
		int sum = 0;
		HashMap hm = new HashMap();
		for (Text user : value) {

			hm.put(user.toString(), 1);

		}
		temp.set(hm.size());
		context.write(key, temp);
	}

}
