package mr05.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class LogProcessorDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        String input = args[0];
        String output = args[1];

        FileSystem fs = FileSystem.get(conf);
        boolean exists = fs.exists(new Path(output));
        if (exists) {
            fs.delete(new Path(output), true);
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(LogProcessorDriver.class);
        job.setJobName("Process Logs");

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(LogProcessorMapper.class);
        job.setReducerClass(LogProcessorReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /*
         * Set up the partitioner. Specify 12 reducers - one for each
         * month of the year. The partitioner class must have a
         * getPartition method that returns a number between 0 and 11.
         * This number will be used to assign the intermediate output
         * to one of the reducers.
         */
        job.setNumReduceTasks(12);

        /*
         * Specify the partitioner class.
         */
        job.setPartitionerClass(LogProcessorMonthPartitioner.class);

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
