package com.cloudwick.developer.avgwordcount;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import com.cloudwick.developer.avgwordcount.WordCountMapper.WordsCount;

public class WordCountReducer extends
		Reducer<Text, FloatWritable, Text, FloatWritable> {

	/**
	 * Calculate the avg word count, i.e Sum of each word /total words
	 */
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> value,
			Context context) throws IOException, InterruptedException {
		Text temp = new Text();
		FloatWritable f = new FloatWritable();
		float sum = 0;
		for (FloatWritable intWritable : value) {
			sum += intWritable.get();
		}
		temp.set(key);
		Counter counter = (Counter) context.getCounter(WordsCount.wordCount);
		long counterValue = counter.getValue();
		f.set(sum/counterValue);
		context.write(temp, f);
	}
}
