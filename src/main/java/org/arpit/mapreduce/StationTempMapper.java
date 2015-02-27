package org.arpit.mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Arpit on 2/27/2015.
 */
public class StationTempMapper extends Mapper<LongWritable,Text, Text, FloatWritable> {

    private GSODTempParser parser = new GSODTempParser();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getStationID()), new FloatWritable(parser.getairTemperature()));
        }
    }
}
