package mr07.reduce_side_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserTrnxsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String DATA_SEPARATOR = context.getConfiguration().get("input.file.separator");
        String transactionFileTag = context.getConfiguration().get("trnx.tag");
        String values[] = value.toString().split(DATA_SEPARATOR);
        StringBuilder dataStringBuilder = new StringBuilder();
        for (int index = 0; index < values.length; index++) {
            if (index != 0) {
                dataStringBuilder.append(values[index].toString().trim() + DATA_SEPARATOR);
            } else {
                dataStringBuilder.append(transactionFileTag);
            }
        }
        String dataString = dataStringBuilder.toString();
        if (dataString != null && dataString.length() > 1) {
            dataString = dataString.substring(0, dataString.length() - 1);
        }
        dataStringBuilder = null;
        context.write(new LongWritable(Long.parseLong(values[0])), new Text(dataString));
    }
}