package solution;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This is the NYSEReducer class
 */ 
public class NYSEReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

  FloatWritable maxValueFloatWritable = new FloatWritable();
  
  @Override
  public void reduce(Text key, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {
	  float maxValue = 0;
	  for(FloatWritable value : values) {
		    float currentValue = value.get();
		    if( currentValue > maxValue) {
		        maxValue = currentValue;
		    }
		}
	  maxValueFloatWritable.set(maxValue);
	  context.write(key, maxValueFloatWritable);
  }
}