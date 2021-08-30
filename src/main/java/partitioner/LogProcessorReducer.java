package partitioner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//count the number of hits per ip
public class LogProcessorReducer extends Reducer<Text, Text, Text, IntWritable> {

    IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numberOfHits = 0;
        //Ignore warnings that we don't use the value
        //In this example we count the values
        for (@SuppressWarnings("unused")
                Text val : values) {
            numberOfHits ++ ;
        }
        result.set(numberOfHits);
        context.write(key, result);
    }

}
