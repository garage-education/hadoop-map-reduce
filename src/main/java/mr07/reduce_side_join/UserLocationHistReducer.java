package mr07.reduce_side_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

public class UserLocationHistReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    private static final String UNKNOWN_CELL = "???,???,???,???";
    private static final String TAG_SEPARATOR = "~";


    @Override
    protected void reduce(LongWritable key, Iterable<Text> values,
                          Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        String DATA_SEPARATOR = context.getConfiguration().get("input.file.separator");
        String transactionFileTag = context.getConfiguration().get("trnx.tag");

        String cellLKP = null;
        String userTransactions = null;
        LinkedList<String> trnxs = new LinkedList<>();

        for (Text txtValue : values) {

            String value = txtValue.toString();
            String[] spllitedValues = value.split(TAG_SEPARATOR);
            String tag = spllitedValues[0];
            String rowValue = spllitedValues[1];
            if (tag.equalsIgnoreCase("TRNX")) {
                trnxs.add(rowValue);
                //userTransactions = rowValue;
            } else if (tag.equalsIgnoreCase("Cell")) {
                cellLKP = rowValue;
            }
        }

        String outputJoin = null;
        for (String trnx : trnxs) {
            if (trnx != null && cellLKP != null) {
                outputJoin = trnx + DATA_SEPARATOR + cellLKP;
                context.write(key, new Text(outputJoin));
            } else if (cellLKP == null) {
                outputJoin = trnx + DATA_SEPARATOR + UNKNOWN_CELL;
                context.write(key, new Text(outputJoin));
            }
        }
        trnxs.clear();

    }

}