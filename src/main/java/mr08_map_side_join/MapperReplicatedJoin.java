package mr08_map_side_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
//Phases lifecycle are setup -> map -> cleanup
//setup -> reduce -> cleanup
//https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for/25450627
//https://lintool.github.io/MapReduceAlgorithms/MapReduce-book-final.pdf
//https://lintool.github.io/MapReduceAlgorithms/ed1.html
public class MapperReplicatedJoin extends Mapper<Object, Text, Text, Text> {
    private static final Text EMPTY_TEXT = new Text("");
    private HashMap<String, String> serviceid_To_pincode = new HashMap<String, String>();
    private Text outvalue = new Text();
    private String joinType = null;
    private String service_id_file_location = null;

    @Override
    public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        String data = values.toString();
        String[] field = data.split(",", -1);
        String service_id = field[0];
        String pincode = serviceid_To_pincode.get(service_id);
        outvalue.set(EMPTY_TEXT);
        if (null != pincode && pincode.length() > 0) {
            outvalue.set(pincode);
            context.write(values, outvalue);
        } else if (joinType.equalsIgnoreCase("leftouter")) {
            context.write(values, EMPTY_TEXT);
        }
    }

    @Override
    public void setup(Context context) {
        joinType = context.getConfiguration().get("join.type");
        service_id_file_location = context.getConfiguration().get("service.id.file.path");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(service_id_file_location));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] field = line.split(",", -1);
                String service_id = field[0];
                String pincode = field[1];
                if (null != service_id && service_id.length() > 0 && null != pincode && pincode.length() > 0) {
                    serviceid_To_pincode.put(service_id, pincode);
                }
            }
            bufferedReader.close();
        } catch (Exception ex) {
            System.out.println(ex.getLocalizedMessage());
        }
    }
}
