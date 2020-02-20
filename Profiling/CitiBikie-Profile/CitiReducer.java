import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CitiReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        
        String coloumn = key.toString();
        if(coloumn.equals("Duration")){
            double sum = 0.0;
            int total = 0;
            double min = 0;
            double max = 0;
            for(DoubleWritable value : values){
                min = (min > value.get()) ? value.get() : min;
                max = (max < value.get()) ? value.get() : max;
                sum = sum + value.get();
                total++;
            }
            double totalInputs = Double.valueOf(total);
            double mean = sum/totalInputs;
            context.write(new Text("TotalDurationInputs"),new Text(Double.toString(totalInputs)));
            context.write(new Text("minDuration"),new Text(Double.toString(min)));
            context.write(new Text("maxDuration"),new Text(Double.toString(max)));
            context.write(new Text("meanDuration"),new Text(Double.toString(mean)));   
        }else if(coloumn.equals("missingDuration")){
            int count = 0;
            for(DoubleWritable value : values){
                count++;
            }
            context.write(new Text("missingDuration"),new Text(Integer.toString(count)));
        }else if(coloumn.equals("Distance")){
            double sum = 0.0;
            int total = 0;
            double min = 0;
            double max = 0;
            for(DoubleWritable value : values){
                min = (min > value.get()) ? value.get() : min;
                max = (max < value.get()) ? value.get() : max;
                sum = sum + value.get();
                total++;
            }
            double totalInputs = Double.valueOf(total);
            double mean = sum/totalInputs;
            context.write(new Text("TotalDistanceInputs"),new Text(Double.toString(totalInputs)));
            context.write(new Text("minDistance"),new Text(Double.toString(min)));
            context.write(new Text("maxDistance"),new Text(Double.toString(max)));
            context.write(new Text("meanDistance"),new Text(Double.toString(mean)));
        }else if(coloumn.equals("missingDOB")){
            int count = 0;
            for(DoubleWritable value : values){
                count++;
            }
            context.write(new Text("NADOB"),new Text(Integer.toString(count)));
        }else if(coloumn.equals("age")){
            double sum = 0.0;
            int total = 0;
            double min = 0;
            double max = 0;
            for(DoubleWritable value : values){
                min = (min > value.get()) ? value.get() : min;
                max = (max < value.get()) ? value.get() : max;
                sum = sum + value.get();
                total++;
            }
            double totalInputs = Double.valueOf(total);
            double mean = sum/totalInputs;
            context.write(new Text("minAge"),new Text(Double.toString(min)));
            context.write(new Text("maxAge"),new Text(Double.toString(max)));
            context.write(new Text("meanAge"),new Text(Double.toString(mean)));
        }else if(coloumn.equals("statioID")){
            HashSet<Double> set = new HashSet<Double>();

            for(DoubleWritable value : values){
                if(!set.contains(value.get())) set.add(value.get());
            }

            context.write(new Text("uniqueStations"), new Text(Integer.toString(set.size())));
        }else if(coloumn.equals("record")){
            int count = 0;
            for(DoubleWritable value : values){
                count++;
            }
            context.write(new Text("totalRecords"),new Text(Integer.toString(count)));
        }
      
        
    }

}

//javac -classpath `yarn classpath` -d . CitiMapper.java 
//javac -classpath `yarn classpath` -d . CitiReducer.java 
//jar -cvf citi.jar *.class
// hadoop jar  citi.jar CitiJob /user/mn2643/FinalProject/data /user/mn2643/FinalProject/output
//hdfs dfs -ls /user/mn2643/FinalProject/output/
//javac -classpath `yarn classpath`:. -d . MaxTemperature.java 
//jar -cvf citiclean.jar *.class