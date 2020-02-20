import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class NYCweatherdataMapperEX1
    extends Mapper<LongWritable, Text, Text,Text> {
    private static final int MISSING = 9999;

public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
String delimeters = "[,]";
        String[] rev=line.split(delimeters);
		String date_format=rev[2];				
		String year=date_format.substring(0,4);
		String month=date_format.substring(4,6);
		String date=date_format.substring(6,8);
		String formated_date= month+"/"+date+"/"+year;
String final_output=formated_date+","+rev[3]+","+rev[5]+","+rev[11]+","+rev[12]+","+rev[15]+","+rev[17]+","+rev[18];

 context.write(new Text("CleanedRecords"), new Text(final_output));
}
}

