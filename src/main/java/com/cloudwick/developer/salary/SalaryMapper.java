package com.cloudwick.developer.salary;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalaryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Text temp = new Text();
		IntWritable age = new IntWritable();
		String line = value.toString();
		String[] words = line.split(",");
		// third column is age
		// get first name and last name
		/*
		 * if(words[2].equalsIgnoreCase("25")){
		 * age.set(Integer.parseInt(words[2])); temp.set(words[0]+":"+words[1]);
		 * context.write(temp,age); }
		 */

		// get the average salary code
		if (words[2].equalsIgnoreCase("25")) {
			age.set(Integer.parseInt(words[3]));
			temp.set("salarysum");
			context.write(temp, age);
		}

	}
}
