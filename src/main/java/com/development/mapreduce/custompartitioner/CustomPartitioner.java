package com.development.mapreduce.custompartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by achilles on 02/03/15.
 */
public class CustomPartitioner extends Partitioner<Text,IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int numberOfPartitioners) {
        String partitonerKey = text.toString();

        if(numberOfPartitioners == 2){
            if(partitonerKey.charAt(0) < 'a')
                return 0;
            else
                return 1;
        }else if (numberOfPartitioners==1){
            return 0;
        }else{
            System.err.println("set number of partitioner to 2");
            return 0;
        }

    }
}
