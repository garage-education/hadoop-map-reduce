package mr03.inverted_index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

    private final Text allFilesConcatValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws java.io.IOException ,InterruptedException {
        StringBuilder filelist = new StringBuilder("");
        for(Text value:values) {
            filelist.append(value.toString()).append(";");
        }
        allFilesConcatValue.set(filelist.toString());
        context.write(key, allFilesConcatValue);
    };
}