package org.arpit.mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.IOException;

/**
 * Created by Arpit on 2/26/2015.
 */
public class MaxTempReducerWithCache extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private GSODStationParser metadata;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        metadata = new GSODStationParser();
        metadata.initialize(new File("isd-history.txt"));
    }

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        String stationName = metadata.getStationName(key.toString());
        Float maxValue = Float.MIN_VALUE;
        for (FloatWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
        }
        context.write(new Text(stationName), new FloatWritable(maxValue));
    }
}
