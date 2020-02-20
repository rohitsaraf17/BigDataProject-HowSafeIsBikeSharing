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
        int len= rev.length;
                String date_format=rev[2];
                String year=date_format.substring(0,4);
                String month=date_format.substring(4,6);
                String date=date_format.substring(6,8);
                String formated_date= month+"/"+date+"/"+year;
String dig=rev[21];
String DWeather="";
if(dig.equals("100000"))
{
  DWeather="Fog";
}
else if(dig.equals("010000"))
{
 DWeather="Rain";
}
else if(dig.equals("001000"))
{
DWeather="Snow/ice pellets";
}
else if(dig.equals("000100"))
{
 DWeather="Hail";
}
else if(dig.equals("000010"))
{
 DWeather="Thunder";
}
else if(dig.equals("000001"))
{
 DWeather="Tornado";
}
else
{
DWeather="0";
}
if(rev[11].equals("999.9"))
{
rev[11]="0";
}
if(rev[13].equals("999.9"))
{
rev[13]="0";
}

String final_output=year+","+month+","+rev[3]+","+rev[5]+","+rev[11]+","+rev[13]+","+rev[17]+","+rev[18]+","+DWeather;
 context.write(null, new Text(final_output));

}
}
