package mr08.map_side_join;

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
public class MapperReplicatedJoin extends Mapper<LongWritable, Text, Text, Text> {
    private static final Text EMPTY_TEXT = new Text("");
    private HashMap<String, String> cellLookup = new HashMap<String, String>();
    private Text outvalue = new Text();
    private String joinType = null;
    private String cellLookupLocation = null;

    @Override
    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        String data = values.toString();
        String DATA_SEPARATOR = ",";
        String[] field = data.split(",");
        String cellId = field[0];
        String cellInfo = cellLookup.get(cellId);
        outvalue.set(EMPTY_TEXT);
        StringBuilder dataStringBuilder = new StringBuilder();
        for (int index = 0; index < field.length; index++) {
            if (index != 0) {
                dataStringBuilder.append(field[index].trim() + DATA_SEPARATOR);
            } else {
                dataStringBuilder.append(cellId+ DATA_SEPARATOR);
            }
        }
        if (cellInfo != null && cellInfo.length() > 0) {
            dataStringBuilder.append(cellInfo);
        } else {
            dataStringBuilder.append("???,???,???,???");
        }

        context.write(new Text(cellId), new Text(dataStringBuilder.toString()));

    }

    @Override
    public void setup(Context context) {
        cellLookupLocation = context.getConfiguration().get("lkp.file.path");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(cellLookupLocation));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] field = line.split("\\|");
                String cellId = field[0];
                String cellInfo = field[1];
                if (null != cellId && cellId.length() > 0) {
                    cellLookup.put(cellId, cellInfo);
                }
            }
            bufferedReader.close();
        } catch (Exception ex) {
            System.out.println(ex.getLocalizedMessage());
        }
    }
}
