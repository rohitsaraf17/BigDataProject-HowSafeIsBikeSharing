import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CitiCleanReducer extends Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        String coloumn = key.toString();
        if(coloumn.equals("SkippedRecords") || coloumn.equals("TotalRecords")){
            int count = 0;
            for(Text value : values){
                count++;
            }

            if(coloumn.equals("SkippedRecords")) context.write(NullWritable.get(), new Text("Skipped Records " + Integer.toString(count)));
            else context.write(NullWritable.get(), new Text("Total Records " + Integer.toString(count))); 
        }else if(coloumn.equals("CleanedRecords")){
            for(Text value : values){
                context.write(NullWritable.get(), value);
            }
        }
      
        
    }

}