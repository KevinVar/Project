import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


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
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, MapWritable> {
		private Text word = new Text();
		
		
		/*The key is the number of the line of a cvs file
		 *The value is the content of the line
		 *Output:
		 *For each word of each row, we gave them, as key, the number of their futur row  
		 *and the value is a map that contains the column number and the word
		 *example: maps() generate:
		 *(0,[0,a]) 				(0,[1,d])					(0,[2,g])
		 * (1,[0,b]) 				(1,[1,e])					(1,[2,h])
		 * (2,[0,c]) 				(2,[1,f])					(2,[2,i]) 
		*/
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, MapWritable> output) throws IOException {
			int row = 0;
			MapWritable tab = new MapWritable();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line,",");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				tab.put(key,word);
				output.collect(row, tab);
				row++;
			}
		}
	}
	
	// Shuffle and sort give:
	//			(0,[[0,a],[1,d],[2,g]])
	//			(1,[[0,b],[1,e],[2,h]])
	//			(2,[[0,c],[1,f],[2,i]])
	
	
	//in the Reduce class we have the reduce function for the pivot 
	 public static class Reduce extends MapReduceBase implements Reducer< IntWritable, MapWritable, IntWritable, Text> {
		 
		 /*
		  * the key and value are the result of the shuffle and sort
		  * Output:
		  * The reduce function generates for each row a string that contains the words at their correct column separated by ','.
		  * example: reduces generate:
		  *(0,'a,d,g')
		  *(1,'b,e,h')
		  *(2,'c,f,i')
		 */
		 public void reduce(IntWritable key, Iterator<MapWritable> values, OutputCollector<IntWritable,Text> output) throws IOException {
			 int count=0;
			 String word="";
			 MapWritable tab = new MapWritable();
			 // for the row 'key', we get all the result values of the shuffle and put them in a single map
			 while (values.hasNext()) {
				 for (  Entry<IntWritable,Text> map : values.next().entrySet()) {
					 tab.put(map.getKey(), map.getValue());}
			 }
			 //we create a string that represent our new row. Example : a,d,g
			 word=word+tab.get(0);
			 for(count=1;count<tab.size;count++){
				 word=word+","+tab.get(count);
			 }		
			 //in the output we put the row number and the string that correspond. Example:(0,'a,d,g')
			 output.collect(key, new Text(word));
		 }
	 }

}

