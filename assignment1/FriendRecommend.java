package FriendRecommend;

import java.io.IOException;  
import java.util.*;  
import java.util.Map.Entry;

import org.apache.commons.lang.*;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.conf.*;  
import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapreduce.*;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
   
public class FriendRecommend{  
	public static class Map extends Mapper<LongWritable,Text,IntWritable,Text>{
		public void map(LongWritable key,Text data,Context context) throws IOException,InterruptedException{
			String input = data.toString();
			String[] splitData = input.split("\t",-1);//split the input by tab
			if(splitData.length==2){//check whether this is a valid line of data
				if(splitData[1].length() != 0){//check whether this user has friend, if not, ignore him.
					IntWritable userKey = new IntWritable(Integer.parseInt(splitData[0]));
					String[] friends = splitData[1].split(",");//split the frined list of current user
					String friend1;
					String friend2;
					IntWritable key1 = new IntWritable();
					IntWritable key2 = new IntWritable();
					Text value1 = new Text();
					Text value2 = new Text();
					for(int i=0;i<friends.length;i++){
						friend1 = friends[i];
						value1.set("1,"+friend1);
						context.write(userKey,value1);//save the 1 degree friends as "a 1,b"
						key1.set(Integer.parseInt(friend1));
						value1.set("2,"+friend1);
						for (int j=i+1;j<friends.length;j++){
							friend2 = friends[j];
							key2.set(Integer.parseInt(friend2));
							value2.set("2,"+friend2);
							context.write(key1,value2);//save the 2 degree friends as "a 2,b" and "b 2,a"
							context.write(key2,value1);
						}
					}
				}
			}
		}
	}
   
	public static class Reduce extends Reducer<IntWritable,Text,IntWritable,Text>{
		public void reduce(IntWritable key, Iterable<Text> friendPair, Context output) throws IOException,InterruptedException{
			String[] friend;
			HashMap<String,Integer>hash = new HashMap<String,Integer>();
			for(Text value:friendPair){
				friend = (value.toString()).split(",");
				if(friend[0].equals("1")){
					hash.put(friend[1],-1);//if they are 1 degree friends, save the data
				}else{//else they are 2 degree friends
					if(hash.containsKey(friend[1])){
						if(hash.get(friend[1])!=-1){//check whether this pair has already been saved as 1 degree friend
							hash.put(friend[1],hash.get(friend[1])+1);//if this friend has been stored, the amount of 2 degree friend between this friend and user plus 1.
						}
					}else{
						hash.put(friend[1], 1);//if this friend has not been stored, store him.
					}
				}
			}
			ArrayList<Entry<String,Integer>> list = new ArrayList<Entry<String,Integer>>();
			for(Entry<String,Integer> entry : hash.entrySet()){  
                if(entry.getValue()!=-1){
                    list.add(entry);//if the friend is not 1 degree friend, store him
				}
            }
			Collections.sort(list,new Comparator<Entry<String,Integer>>(){
                public int compare(Entry<String,Integer> entry1,Entry<String,Integer>entry2){
                    return entry1.getValue().compareTo(entry2.getValue());//sort by 2 degree friends amount
				}
            });
			ArrayList<Integer>sameLevel = new ArrayList<Integer>();//save the people who have same amount of 2 degree friends. later sort them by name.
			ArrayList<Integer>fo = new ArrayList<Integer>();
			if(list.size()==1)  {
                output.write(key, new Text(StringUtils.join(list,",")));//if only 1 recommend friend has been stored immedia
			}
			else if(list.size()>1)
			{
				sameLevel.add(Integer.parseInt(list.get(0).getKey()));//use temperory list to save the people with same number of mutural friends
				for(int i=1;i<Math.min(10,list.size());i++){
					if(list.get(i).getValue()==list.get(i-1).getValue()){
						sameLevel.add(Integer.parseInt(list.get(i).getKey()));
					}else{
						Collections.sort(sameLevel);
						for(int j=0;j<sameLevel.size();j++)
							fo.add(sameLevel.get(j));
						sameLevel.clear();
						sameLevel.add(Integer.parseInt(list.get(i).getKey()));//if the number has changed update the temperory list's data to main output list. then clear temperory list and save the new data
					}
                }
				Collections.sort(sameLevel);
				for(int j=0;j<sameLevel.size();j++)
					fo.add(sameLevel.get(j));
                output.write(key,new Text(StringUtils.join(fo,",")));
			}
		}
	}
	
	public static void main(String[]args) throws Exception{
        Configuration conf = new Configuration();
		Job job = new Job(conf,"FriendRecommend");
        job.setJarByClass(FriendRecommend.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);  
    }
}