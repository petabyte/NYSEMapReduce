package solution;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestNYSEMapReduce {

    /*
     * Declare harnesses that let you test a mapper, a reducer, and
     * a mapper and a reducer working together.
     */
    MapDriver<LongWritable, Text, Text, FloatWritable> mapDriver;
    ReduceDriver<Text, FloatWritable, Text, FloatWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable> mapReduceDriver;
    Partitioner<Text, FloatWritable> nysePartitioner;

    /*
     * Set up the test. This method will be called before every test.
     */
    @Before
    public void setUp() {

    /*
     * Set up the mapper test harness.
     */
        NYSEMapper mapper = new NYSEMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, FloatWritable>();
        mapDriver.setMapper(mapper);

    /*
     * Set up the reducer test harness.
     */
        NYSEReducer reducer = new NYSEReducer();
        reduceDriver = new ReduceDriver<Text, FloatWritable, Text, FloatWritable>();
        reduceDriver.setReducer(reducer);

        /**
         * Partitioner Test
         */
        nysePartitioner = new NYSEPartitioner<Text, FloatWritable>();
    
    /*
     * Set up the mapper/reducer test harness.
     */
        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, FloatWritable, Text, FloatWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    /*
     * Test the mappers.
     */
    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text("NYSE,ZTR,2010-02-08,3.75,3.81,3.72,3.78,298200,3.78"))
                .withInput(new LongWritable(1), new Text("NYSE,ZTR,2010-02-08,3.75,3.81,3.80,3.78,298200,3.78"))
                .withInput(new LongWritable(1), new Text("NYSE,AAA,2010-02-08,3.75,3.81,3.72,3.78,298200,3.78"))
                .withInput(new LongWritable(1), new Text("Did you check for clean data?"))
                        //Division by zero test case
                .withInput(new LongWritable(1), new Text("NYSE,AAA,2010-02-08,3.75,3.81,0,3.78,298200,3.78"));
        mapDriver.addOutput(new Text("ZTR"), new FloatWritable((((float) 3.81 - (float) 3.72) * 100 / (float) 3.72)));
        mapDriver.addOutput(new Text("ZTR"), new FloatWritable((((float) 3.81 - (float) 3.80) * 100 / (float) 3.80)));
        mapDriver.addOutput(new Text("AAA"), new FloatWritable((((float) 3.81 - (float) 3.72) * 100 / (float) 3.72)));
        mapDriver.runTest();
    }

    /**
     * Test Partitioner
     */

    @Test
    public void testPartitioner() {
        Text keyText = new Text();
        FloatWritable floatWritable = new FloatWritable();
        keyText.set("AAA");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 0);
        keyText.set("BBB");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 1);
        keyText.set("CCC");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 2);
        keyText.set("DDD");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 3);
        keyText.set("EEE");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 4);
        keyText.set("FFF");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 5);
        keyText.set("GGG");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 6);
        keyText.set("HHH");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 7);
        keyText.set("III");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 8);
        keyText.set("JJJ");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 9);
        keyText.set("KKK");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 10);
        keyText.set("LLL");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 11);
        keyText.set("MMM");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 12);
        keyText.set("NNN");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 13);
        keyText.set("OOO");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 14);
        keyText.set("PPP");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 15);
        keyText.set("QQQ");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 16);
        keyText.set("RRR");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 17);
        keyText.set("SSS");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 18);
        keyText.set("TTT");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 19);
        keyText.set("UUU");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 20);
        keyText.set("VVV");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 21);
        keyText.set("WWW");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 22);
        keyText.set("XXX");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 23);
        keyText.set("YYY");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 24);
        keyText.set("ZZZ");
        assertTrue(nysePartitioner.getPartition(keyText, floatWritable, 25) == 25);
    }


    /*
     * Test the reducer.
     */
    @Test
    public void testReducer() throws IOException {

        List<FloatWritable> values = new ArrayList<FloatWritable>();
        values.add(new FloatWritable(37.00f));
        values.add(new FloatWritable(42.00f));
        values.add(new FloatWritable(37.00f));
        values.add(new FloatWritable(42.00f));
        reduceDriver.withInput(new Text("AAA"), values);
        reduceDriver.withOutput(new Text("AAA"), new FloatWritable(42.00f));
        reduceDriver.runTest();

    }


    /*
     * Test the mapper and reducer working together.
     */
    @Test
    public void testMapReduce() throws IOException {

        mapReduceDriver.withInput(new LongWritable(1), new Text(
                "NYSE,ZTR,2010-02-08,3.75,3.81,3.72,3.78,298200,3.78"))
                .withInput(new LongWritable(1), new Text(
                        "NYSE,ZTR,2010-02-08,3.75,25.00,5.00,3.78,298200,3.78"))
                .withInput(new LongWritable(1), new Text(
                        "NYSE,AAA,2010-02-08,3.75,30.0,15.00,3.78,298200,3.78"))
                .withInput(new LongWritable(1), new Text(
                        "NYSE,AAA,2010-02-08,3.75,30.00,5.00,3.78,298200,3.78"));
        mapReduceDriver.withOutput(new Text("AAA"), new FloatWritable(500f));
        mapReduceDriver.withOutput(new Text("ZTR"), new FloatWritable(400f));
        mapReduceDriver.runTest();

    }
}
