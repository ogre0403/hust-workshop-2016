package labs.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ogre0403 on 2016/2/26.
 */
public class WordCountReducer  extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        IntWritable result = new IntWritable();
        for (IntWritable val : values) {
            /**
             * Labs 1:
             * Iterate all elements in values, and sum up all IntWritable objects
             */
        }
        result.set(sum);
        context.write(key, result);
    }
}
