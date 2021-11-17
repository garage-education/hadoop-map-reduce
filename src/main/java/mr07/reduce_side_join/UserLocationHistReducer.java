package mr07.reduce_side_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class UserLocationHistReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    public static final String TAG_SEPARATOR = "~";
    private static final String DATA_SEPARATOR = ",";

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values,
                          Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        String cellLKP = null;
        String userTransactions = null;
        //LinkedList<String> trnxs = new LinkedList<>();

        for (Text txtValue : values) {

            String value = txtValue.toString();
            String[] spllitedValues = value.split(TAG_SEPARATOR);
            String tag = spllitedValues[0];
            String rowValue = spllitedValues[1];
            if (tag.equalsIgnoreCase("TRNX")) {
                userTransactions=rowValue;
            } else if (tag.equalsIgnoreCase("Cell")) {
                cellLKP = rowValue;
            }
        }

        String outputJoin = null;
        if (userTransactions != null && cellLKP != null) {
            outputJoin = userTransactions + DATA_SEPARATOR + cellLKP;
        } else if (cellLKP == null) {
            outputJoin = userTransactions + DATA_SEPARATOR + "???,???,???,???";
        }
        context.write(key, new Text(outputJoin));
    }

}