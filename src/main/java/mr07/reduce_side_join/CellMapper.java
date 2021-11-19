package mr07.reduce_side_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CellMapper extends Mapper<LongWritable, Text, LongWritable, Text> {



    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String DATA_SEPARATOR = context.getConfiguration().get("input.file.separator");
        String cellFileTag =context.getConfiguration().get("cell.lkp.tag");
        String values[] = value.toString().split(DATA_SEPARATOR);
        StringBuilder dataStringBuilder = new StringBuilder();
        for (int index = 0; index < values.length; index++) {
            if (index != 0) {
                dataStringBuilder.append(values[index].trim() + DATA_SEPARATOR);
            } else {
                dataStringBuilder.append(cellFileTag);
            }
        }
        String dataString = dataStringBuilder.toString();

        dataStringBuilder = null;
        context.write(new LongWritable(Long.parseLong(values[0])), new Text(dataString));
    }
}