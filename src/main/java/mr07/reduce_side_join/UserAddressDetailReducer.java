package mr07.reduce_side_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserAddressDetailReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    public static final String TAG_SEPARATOR = "~";
    private static final String DATA_SEPARATOR = ",";

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values,
                          Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        String value;
        String[] spllitedValues;
        String tag;
        String data = null, userDetails = null, addressDetails = null;
        for (Text txtValue : values) {

            value = txtValue.toString();
            spllitedValues = value.split(TAG_SEPARATOR);
            tag = spllitedValues[0];
            if (tag.equalsIgnoreCase("UD")) {
                userDetails = spllitedValues[1];
            } else if (tag.equalsIgnoreCase("AD")) {
                addressDetails = spllitedValues[1];
            }
        }
        if (userDetails != null && addressDetails != null) {
            data = userDetails + DATA_SEPARATOR + addressDetails;
        } else if (userDetails == null) {
            data = addressDetails;
        } else if (addressDetails == null) {
            data = userDetails;
        }
        context.write(key, new Text(data));
    }
}