package com.svsg.sample.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * This class uses as a mapper class that inherits {@link Mapper}.
 * The Mapper class is a generic type, with four formal type parameters that specify the input key, input value,
 * output key, and output value types of the map function. For the present example, the input key is a long integer
 * offset, the input value is a line of text, the output key is a year, and the output value is an air temperature (an integer).
 * Be aware that we can override more methods to handle or mapper better like {@link Mapper#cleanup(Context)}
 * for run after mapper do its job, and {@link Mapper#setup(Context)} to initialize our Mapper.
 * For advance job we can use {@link Mapper#run(Context)}
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    /**
     *
     * @param key key of input, in this case it's the line of the input
     * @param value value of input, in this case it's a line of input
     * @param context contains Key/Value(s) of input, and configuration can be passed over this object.
     *                It handles the output too.
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
