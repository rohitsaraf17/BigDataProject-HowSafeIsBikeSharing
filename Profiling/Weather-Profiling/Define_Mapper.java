import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Define_Mapper
    extends Mapper<LongWritable, Text, Text,Text> {
    private static final int MISSING = 9999;

public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
String delimeters = "[,]";
        String[] rev=line.split(delimeters);
int STN=0;
int WBAN=0;
String date="";
double temp=0.0;
double dew=0.0;
double slp=0.0;
double stp=0.0;
double visiblity=0.0;
double windspeed=0.0;
double gust=0.0;
String max="";
String min="";
double prcp=0.0;
double sndp=0.0;
double weather_State=0.0;
double max_speed=0.0;

if(!rev[2].equals("NULL"))
{
date=rev[2];
}
if(!rev[3].equals("NULL") || !rev[3].matches(".*[A-Za-z].*"))
{
temp=Double.parseDouble(rev[3]);
}
if(!rev[5].equals("NULL") || !rev[5].matches(".*[A-Za-z].*"))
{
dew=Double.parseDouble(rev[5]);
}
if(!rev[11].equals("NULL") || !rev[11].matches(".*[A-Za-z].*"))
{
visiblity=Double.parseDouble(rev[11]);
}
if(!rev[12].equals("NULL") || !rev[12].matches(".*[A-Za-z].*"))
{
windspeed=Double.parseDouble(rev[12]);
}
if(!rev[15].equals("NULL") || !rev[15].matches(".*[A-Za-z].*"))
{
max_speed=Double.parseDouble(rev[15]);
}

if(!rev[17].equals("NULL") || !rev[17].matches(".*[A-Za-z].*"))
{
max=rev[17];
}
if(!rev[18].equals("NULL") || !rev[18].matches(".*[A-Za-z].*"))
{
min=rev[18];
}

String final_output=date+","+ Double.toString(temp)+","+ Double.toString(dew)+","+ Double.toString(visiblity)+","+Double.toString(windspeed)+","+Double.toString(max_speed) +","+max+","+min;

 context.write(new Text("Defining Records"), new Text(final_output));
}


}
