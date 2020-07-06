package Project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SpatialJoin {

    public static class PointMapper extends Mapper<LongWritable, Text, Text, Text> {
    	//String window;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] point = value.toString().split(",");
            int pX = Integer.parseInt(point[0]);
            int pY = Integer.parseInt(point[1]);
            int zoneHeight = 20;
            int numZones = 10000/zoneHeight;
            int minHeight = 1;
            String zone = new String();
            int wblX = 0;
            int wblY = 0;
            int wtrX = 0;
            int wtrY = 0;
            Configuration conf = context.getConfiguration();
            String window = conf.get("w");
            if (window != null && window != "") {
            	String[] windowDims = window.split(",");
            	wblX = Integer.parseInt(windowDims[0]);
            	wblY = Integer.parseInt(windowDims[1]);
            	wtrX = Integer.parseInt(windowDims[2]);
            	wtrY = Integer.parseInt(windowDims[3]);
            }
            for (int i = 1; i <= numZones; i++) {
                int zoneHigh = minHeight + (i * zoneHeight);
                int zoneLow = minHeight + ((i-1) * zoneHeight);
                if (pY >= zoneLow && pY <= zoneHigh) {
                    zone = "zone_" + Integer.toString(i);
                }
            }
            if (window == null || window == "" || ((wblX <= pX) && (wtrX >= pX) && (wblY <= pY) && (wtrY >= pY))) {
                context.write(new Text(zone), new Text(pX + "," + pY));
            }
        }
    }

    public static class RectangleMapper extends Mapper<LongWritable, Text, Text, Text> {
        //private String window;
    	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rect = value.toString().split(",");
            int rblX = Integer.parseInt(rect[0]);
            int rblY = Integer.parseInt(rect[1]);
            int rtrX = Integer.parseInt(rect[3]) + rblX;
            int rtrY = Integer.parseInt(rect[2]) + rblY;
            int rtlX = rblX;
            int rtlY = rtrY;
            int rbrX = rtrX;
            int rbrY = rblY;
            boolean rectInWindow;
            int zoneHeight = 20;
            int numZones = 10000/zoneHeight;
            int minHeight = 1;
            String zone;
            int wblX = 0;
            int wblY = 0;
            int wtrX = 0;
            int wtrY = 0;
            Configuration conf = context.getConfiguration();
            String window = conf.get("w");
            if (window != null && window != "") {
            	String[] windowDims = window.split(",");
            	wblX = Integer.parseInt(windowDims[0]);
            	wblY = Integer.parseInt(windowDims[1]);
            	wtrX = Integer.parseInt(windowDims[2]);
            	wtrY = Integer.parseInt(windowDims[3]);
            }
            if ((window != null || window != "") && (wblX <= rblX) && (wtrX >= rblX) && (wblY <= rblY) && (wtrY >= rblY))
                rectInWindow = true;
            else if ((window != null || window != "") && (wblX <= rtrX) && (wtrX >= rtrX) && (wblY <= rtrY) && (wtrY >= rtrY))
                rectInWindow = true;
            else if ((window != null || window != "") && (wblX <= rtlX) && (wtrX >= rtlX) && (wblY <= rtlY) && (wtrY >= rtlY))
                rectInWindow = true;
            else if ((window != null || window != "") && (wblX <= rbrX) && (wtrX >= rbrX) && (wblY <= rbrY) && (wtrY >= rbrY))
                rectInWindow = true;
            else
                rectInWindow = false;
            if (window == null || window == "" || rectInWindow == true) {
                for (int i = 1; i <= numZones; i++) {
                    int zoneHigh = minHeight + (i * zoneHeight);
                    int zoneLow = minHeight + ((i-1) * zoneHeight);
                    if ((rblY >= zoneLow && rblY <= zoneHigh) || (rtrY >= zoneLow && rtrY <= zoneHigh)) {
                        zone = "zone_" + Integer.toString(i);
                        context.write(new Text(zone), new Text(rblX + "," + rblY + "," + rtrX + "," + rtrY));
                    }
                }
            }
        }
    }

    public static class CustomReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> allPoints = new ArrayList<>();
            ArrayList<String> allRectangles = new ArrayList<>();
            for (Text value : values) {
                String[] pr = value.toString().split(",");
                if (pr.length == 2) {
                    allPoints.add(value.toString());
                }
                else {
                    allRectangles.add(value.toString());
                }
            }
            for (String point : allPoints) {
                String[] pointData = point.split(",");
                int pX = Integer.parseInt(pointData[0]);
                int pY = Integer.parseInt(pointData[1]);
                for (String rectangle : allRectangles) {
                    String[] rectangleData = rectangle.split(",");
                    int rblX = Integer.parseInt(rectangleData[0]);
                    int rblY = Integer.parseInt(rectangleData[1]);
                    int rtrX = Integer.parseInt(rectangleData[2]);
                    int rtrY = Integer.parseInt(rectangleData[3]);
                    if ((rblX <= pX) && (rtrX >= pX) && (rblY <= pY) && (rtrY >= pY)) {
                        int rHeight = rtrY - rblY;
                        int rWidth = rtrX - rblX;
                        context.write(new Text(rblX + "," + rblY + "," + rHeight + "," + rWidth), new Text("(" + pX + "," + pY + ")"));
                    }
                }   
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[2]), true);
        conf.set("w", args[3]);
        Job job = new Job(conf);
        job.setJarByClass(SpatialJoin.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectangleMapper.class);
        job.setReducerClass(CustomReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean jobStatus = job.waitForCompletion(true);
        System.exit(jobStatus ? 0 : 1);
    }
}
