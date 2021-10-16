package mr03.inverted_index;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class IndexDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage IndexDriver <input_dir> <output_dir>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        String input = args[0];
        String output = args[1];

        FileSystem fs = FileSystem.get(conf);
        boolean exists = fs.exists(new Path(output));
        if(exists) {
            fs.delete(new Path(output), true);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(IndexDriver.class);

        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
