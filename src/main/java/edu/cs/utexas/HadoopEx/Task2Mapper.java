package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<Object, Text, Text, IntWritable> {

    // Create a hadoop text object to store airline
    private Text airline = new Text();

    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {

        try {    
			String[] cols = value.toString().split(",");
            airline.set(cols[4]);
            context.write(airline, new IntWritable(Integer.parseInt(cols[11])));
        } catch (NumberFormatException e) {
            return;
        }
    } 
}
