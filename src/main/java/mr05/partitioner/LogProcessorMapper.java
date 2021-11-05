package mr05.partitioner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogProcessorMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static List<String> monthsList = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");

    /**
     * Data source from: https://github.com/elastic/examples/tree/master/Common%20Data%20Formats/apache_logs
     *
     * Example line:[83.149.9.216 - - [17/May/2015:10:05:03 +0000] "GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1" 200 203023
     * "http://semicomplete.com/presentations/logstash-monitorama-2013/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko)
     * Chrome/32.0.1700.77 Safari/537.36"]
     */
    //@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Split input line on space
        String[] fields = value.toString().split(" "); //around 34 element


        if (fields.length > 3) {
            //83.149.9.216 - - [17/May/2015:10:05:03 +0000]
            //0-> 83.149.9.216
            //1-> -
            //2-> -
            //3-> [17/May/2015:10:05:03 +0000]
            String ipAddress = fields[0];

            //split the fourth element in the array using "/"
            //0-> [17
            //1-> May
            //2-> 2015:10:05:03 +0000]
            String[] dateFields = fields[3].split("/");
            if (dateFields.length > 1) {
                String theMonth = dateFields[1];
                // check if the month is in a valid format or not.
                if (monthsList.contains(theMonth))
                    context.write(new Text(ipAddress), new Text(theMonth));
            }
        }
    }
}
