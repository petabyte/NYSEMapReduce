package solution;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner
 * @param <K2>
 * @param <V2>
 */
public class NYSEPartitioner<K2, V2> extends Partitioner<Text, FloatWritable> {
    public int getPartition(Text key, FloatWritable value, int
            numReduceTasks) {
        return (key.toString().toLowerCase().charAt(0) - 'a') % 26;
    }
}
