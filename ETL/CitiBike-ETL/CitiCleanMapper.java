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

public class CitiCleanMapper extends Mapper<LongWritable, Text, Text, Text> {

    // Function to Calculate Distance
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

        double age = 0.0;
        double duration = 0;
        double distance = 0;
        int startId  = 0;
        double endId = 0;
        String latitude1 = "";
        String latitude2 = "";
        String longi1 = "";
        String longi2 = "";

        String startStationName = data[4].trim();
        String endStationName = data[8].trim();
        if(!data[0].equals("NULL")){
            data[0] = data[0].replace("\"", "");
            duration = Double.parseDouble(data[0]);
            //context.write(new Text("Duration"),new DoubleWritable(duration));
        }
        
        if(!data[3].contains("NULL") || !data[3].matches(".*[A-Za-z].*") ){
            data[3] = data[3].replace("\"", "");
            startId = Integer.parseInt(data[3]);
        }

        if(!data[7].contains("NULL") || !data[7].matches(".*[A-Za-z].*") ){
            data[7] = data[7].replace("\"", "");
            endId = Double.parseDouble(data[7]);
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

            latitude1 = Double.toString(startlatitude);
            latitude2 = Double.toString(endlatitude);
            longi1  = Double.toString(startlongitude);
            longi2 = Double.toString(endlongitude);
    
            distance = haversine(startlatitude,startlongitude,endlatitude,endlongitude);
            }
        
        if(data[13].length() > 3 && data[13].matches("[0-9]+")){
            data[13] = data[13].replace("\"", "");
            age = 2019.0 - Double.parseDouble(data[13]);
            //context.write(new Text("age"),new DoubleWritable(age));
        }

        //Scheme: Start Time/Date, End Time/Date, Start Station ID, End Station ID , Duration of Trip, Start Latitude, End Latitiude, Start Longitude, End Longitude, Age
        if(age<=5 || age>=90 || distance <= 0 || distance > 20 || duration > 7200 || duration < 600 || startId == 0 || endId <= 0 || endId > 10000){
            context.write(new Text("SkippedRecords"), new Text(Integer.toString(1)));
        }else {
            String finalOutput = data[1] + ","+ data[2] +"," +Double.toString(distance)+","+Integer.toString(startId)+ ","+ Double.toString(endId) + ","+ Double.toString(duration) + ","+ latitude1 + ","+ latitude2 + ","+ longi1 + ","+ longi2 + ","+ Double.toString(age) + ","+startStationName+","+endStationName;
            context.write(new Text("CleanedRecords"), new Text(finalOutput));
        }

        context.write(new Text("TotalRecords"), new Text(Integer.toString(1)));

      
    }

}

//hadoop jar citiclean.jar CitiCleanJob /user/mn2643/FinalProject/data /user/mn2643/FinalProject/clean/
//javac -classpath `yarn classpath` -d . CitiCleanMapper.java
//jar -cvf citiclean.jar *.class

///home/mn2643/FinalProject/HiveOutput