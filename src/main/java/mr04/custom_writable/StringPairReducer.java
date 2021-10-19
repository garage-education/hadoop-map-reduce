package mr04.custom_writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StringPairReducer
        extends Reducer<StringPairWritable, LongWritable, StringPairWritable, LongWritable> {

    private final LongWritable result = new LongWritable();

    @Override
    public void reduce(StringPairWritable key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
