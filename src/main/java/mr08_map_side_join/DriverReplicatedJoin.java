package mr08_map_side_join;

import mr06.secondary_sort.SecondarySortDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class DriverReplicatedJoin extends Configured implements Tool {
    private static Logger logger = Logger.getLogger(DriverReplicatedJoin.class);

    private static final String DATA_SEPARATOR = ",";

    public int run(String[] args) throws Exception {
        String inputTransactionData = args[0];
        String inputLookupData = args[1];
        String output = args[2];
        Configuration conf = new Configuration();
        conf.set("join.type", "inner");
        conf.set("lkp.file.path", inputLookupData);
        conf.set("mapreduce.output.textoutputformat.separator", DATA_SEPARATOR);

        FileSystem fs = FileSystem.get(conf);
        boolean exists = fs.exists(new Path(output));
        if (exists) {
            fs.delete(new Path(output), true);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(DriverReplicatedJoin.class);
        job.setJobName("Replicated Map Join Join");
        FileInputFormat.addInputPath(job, new Path(inputTransactionData));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(MapperReplicatedJoin.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean status = job.waitForCompletion(true);
        logger.info("run(): status="+status);
        return status ? 0 : 1;
    }

    /**
     * The main driver for word count map/reduce program.
     * Invoke this method to submit the map/reduce job.
     *
     * @throws Exception When there is communication problems with the job tracker.
     */
    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 2 parameters
        if (args.length != 3) {
            logger.warn("DriverReplicatedJoin <input-trnx-dir> <input-lkp-dir> <output-dir>");
            throw new IllegalArgumentException("DriverReplicatedJoin <input-trnx-dir> <input-lkp-dir> <output-dir>");
        }


        int returnStatus = submitJob(args);
        logger.info("returnStatus=" + returnStatus);

        System.exit(returnStatus);
    }


    /**
     * The main driver for word count map/reduce program.
     * Invoke this method to submit the map/reduce job.
     *
     * @throws Exception When there is communication problems with the job tracker.
     */
    public static int submitJob(String[] args) throws Exception {

        int returnStatus = ToolRunner.run(new SecondarySortDriver(), args);
        return returnStatus;
    }
}
