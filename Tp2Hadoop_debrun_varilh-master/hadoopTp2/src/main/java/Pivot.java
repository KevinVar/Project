import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * We want to implement the functions map et reduce in such a way to get a Pivot.
 * Example:
 * if we have in input a cvs file with 3 lines:
 *  (0,'a,b,c')
 *  (1,'d,e,f')
 *  (2,'g,h,i')
 *
 *  the output should be
 *  (0,'a,d,g')
 *  (1,'b,e,h')
 *  (2,'c,f,i')
 *
 * */

public class Pivot {

    //in the Map class we have the map function for the pivot
    public static class MapPivot extends Mapper<LongWritable, Text, LongWritable, MapWritable> {
        private Text word = new Text();


        /*The key is the byte offset of the line of a cvs file
         *The value is the content of the line
         *Output:
         *For each word of each row, we gave them, as key, the number of their futur row
         *and the value is a map that contains the byte offset and the word
         *example: maps() generate:
         *(0,[0,a]) 				(0,[1,d])					(0,[2,g])
         * (1,[0,b]) 				(1,[1,e])					(1,[2,h])
         * (2,[0,c]) 				(2,[1,f])					(2,[2,i])
        */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            long row = 0;
            MapWritable tab = new MapWritable();
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,",");
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                tab.put(key,word);
                context.write(new LongWritable(row), tab);
                row++;
            }
        }
    }

    // Shuffle and sort give:
    //			(0,[[0,a],[1,d],[2,g]])
    //			(1,[[0,b],[1,e],[2,h]])
    //			(2,[[0,c],[1,f],[2,i]])

    //in the Reduce class we have the reduce function for the pivot
    public static class Reduce extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
    /*
         * the key and value are the result of the shuffle and sort
         * Output:
         * The reduce function generates for each row a string that contains the words at their correct column separated by ','.
         * example: reduces generate:
         *(0,'a,d,g')
         *(1,'b,e,h')
         *(2,'c,f,i')
        */

        public void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            String word="";
            Map<Long, String> tab = new HashMap();
            long min = 0;
            // for the row 'key', we get all the result values of the shuffle and put them in a single map
            for(MapWritable val : values){
                for (  Entry<Writable,Writable> map : val.entrySet()) {
                    LongWritable longtab=(LongWritable) map.getKey();
                    tab.put(new Long(longtab.get()), map.getValue().toString());}
            }
            //min represents the key of the value to add to the next column
            min=Collections.min(tab.keySet());
            //we create a string that represent our new row. Example : a,d,g
            word=word+tab.get(new Long(0));

            tab.remove(min);
            while(!tab.isEmpty()){
                min=Collections.min(tab.keySet());
                word=word+","+tab.get(min);
                tab.remove(min);
            }
            //in the output we put the row number and the string that correspond. Example:(0,'a,d,g')
            context.write(key, new Text(word));
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Pivot");
        job.setJarByClass(Pivot.class);
        job.setMapperClass(Pivot.MapPivot.class);
        //job.setCombinerClass(Pivot.Reduce.class);
        job.setReducerClass(Pivot.Reduce.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

