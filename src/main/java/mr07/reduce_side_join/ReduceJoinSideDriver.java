package mr07.reduce_side_join;

import mr06.secondary_sort.SecondarySortDriver;
import mr08.map_side_join.DriverReplicatedJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ReduceJoinSideDriver extends Configured implements Tool {
    private static final String DATA_SEPARATOR = ",";
    private static Logger logger = Logger.getLogger(DriverReplicatedJoin.class);

    public int run(String[] args) throws Exception {
        String userDetailData = args[0];
        String AddressLkp = args[1];
        String output = args[2];
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", DATA_SEPARATOR);
        FileSystem fs = FileSystem.get(conf);
        boolean exists = fs.exists(new Path(output));
        if (exists) {
            fs.delete(new Path(output), true);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("ReduceSideJoin App");
        job.setJarByClass(ReduceJoinSideDriver.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(UserAddressDetailReducer.class);
        MultipleInputs.addInputPath(job, new Path(userDetailData), TextInputFormat.class, UserDetailMapper.class);
        MultipleInputs.addInputPath(job, new Path(AddressLkp), TextInputFormat.class, AddressMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
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
        // Make sure there are exactly 3 parameters
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

