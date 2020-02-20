//Delete everything which last for more than 2 hours. or 7200 seconds.
//Find maximum age_value

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.math.*;
import org.apache.hadoop.io.DoubleWritable;
import java.lang.Math;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CitiMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public static final double R = 6372.8;
    public static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);
 
        double a = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.asin(Math.sqrt(a));
        return R * c;
    }
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() == 0) return;
        String line = value.toString();
        String data[] = line.split(",");

        if(data[0].equals("NULL")){
            context.write(new Text("missingDuration"),new DoubleWritable(1.0));
        }else {
            data[0] = data[0].replace("\"", "");
            double duration = Double.parseDouble(data[0]);
            context.write(new Text("Duration"),new DoubleWritable(duration));
        }
        
        if(!data[3].contains("NULL")){
            data[3] = data[3].replace("\"", "");
            context.write(new Text("statioID"), new DoubleWritable(Double.parseDouble(data[3])));
        }
        
        
        if(!data[5].matches(".*[A-Za-z].*") && !data[6].matches(".*[A-Za-z].*") && !data[9].matches(".*[A-Za-z].*") && !data[10].matches(".*[A-Za-z].*")){
            data[5] = data[5].replace("\"", "");
            double startlatitude = Double.parseDouble(data[5]);
           
            data[6] = data[6].replace("\"", "");
            data[9] = data[9].replace("\"", "");
            data[10] = data[10].replace("\"", "");
            double startlongitude = Double.parseDouble(data[6]);
    
            double endlatitude = Double.parseDouble(data[9]);
            double endlongitude = Double.parseDouble(data[10]);
    
            double distance = haversine(startlatitude,startlongitude,endlatitude,endlongitude);
            context.write(new Text("Distance"),new DoubleWritable(distance));
    
            }
        
        if(data[13].length() > 3 && data[13].matches("[0-9]+")){
            data[13] = data[13].replace("\"", "");
            double age = 2019.0 - Double.parseDouble(data[13]);
            context.write(new Text("age"),new DoubleWritable(age));
            
        }else {
            context.write(new Text("missingDOB"),new DoubleWritable(1.0));
        }

            context.write(new Text("record"), new DoubleWritable(1.0));
    }

}