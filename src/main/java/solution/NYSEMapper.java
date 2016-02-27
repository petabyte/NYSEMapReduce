package solution;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is the NYSEMapper.
 */
public class NYSEMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    Text stockSymbolText = new Text();
    FloatWritable percentageChangeWritable = new FloatWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] recordSplit = line.split(",");
        try {
            String stockSymbol = recordSplit[1];
            String priceHigh = recordSplit[4];
            String priceLow = recordSplit[5];
            float priceHighFloat = Float.parseFloat(priceHigh);
            float priceLowFloat = Float.parseFloat(priceLow);
            if (priceLowFloat == 0) {
                throw new Exception("Division by zero");
            }
            float percentageChangeFloat = ((priceHighFloat - priceLowFloat) * 100) / priceLowFloat;
            stockSymbolText.set(stockSymbol);
            percentageChangeWritable.set(percentageChangeFloat);
            context.write(stockSymbolText, percentageChangeWritable);
        } catch (ArrayIndexOutOfBoundsException aie) {
            System.err.println("Ignoring corrupt input: " + value);
        } catch (NumberFormatException nfe) {
            System.err.println("Ignoring corrupt input: " + value);
        } catch (Exception e) {
            System.err.println("Ignoring corrupt input: " + value);
        }
    }
}