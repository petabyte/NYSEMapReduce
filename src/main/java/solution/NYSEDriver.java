package solution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by George on 2/27/2016.
 */
public class NYSEDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: NYSEDriver <input dir> <output dir>\n");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(NYSEDriver.class);
        job.setJobName("NYSE Driver");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NYSEMapper.class);
        job.setReducerClass(NYSEReducer.class);
        job.setPartitionerClass(NYSEPartitioner.class);
        job.setNumReduceTasks(26);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /*
     * The main method calls the ToolRunner.run method, which
     * calls an options parser that interprets Hadoop command-line
     * options and puts them into a Configuration object.
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new NYSEDriver(), args);
        System.exit(exitCode);
    }
}
