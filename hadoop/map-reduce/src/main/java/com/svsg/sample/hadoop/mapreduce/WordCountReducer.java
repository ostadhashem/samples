package com.svsg.sample.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Works as a reducer for final result.
 * In this case Reducer can be use as a Combiner too. Because it can optimize the result of Mapper.
 * Like Mapper, four formal type parameters are used to specify the input and output types, this time for the reduce
 * function. The input types of the reduce function must match the output types of the map function: Text and IntWritable.
 * And in this case, the output types of the reduce function are Text and IntWritable.
 * In addition we can override {@link Reducer#setup(Context)} and {@link Reducer#cleanup(Context)}
 * for initializing and finalizing.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    /**
     * Like Mapper it gets key but values of that key. and finally it uses context to write the result, context can be
     * used to retrieve information about job.
     * @param key key of input value
     * @param values values of this key
     * @param context context of job
     * @throws IOException
     * @throws InterruptedException
     */

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
