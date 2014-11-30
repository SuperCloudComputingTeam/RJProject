package com.company;

/**
 * Created by San on 11/10/2014.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.hash.Hash;

//We can now begin tetsing

public class PartitionJoin extends Configured implements Tool{

    public static class PartitionMapper extends Mapper<Object, Text, IntWritable, Text> {
        //initiate a look up table accessible for all
        //private org.apache.hadoop.io.MapWritable<Text, IntArrayWritable> lookupTableTwo = new MapWritable(Text, IntArrayWritable);
        //private static HashMap<String, String> lookupTable = new HashMap<String, String>();

        //size information for S and T table
        private IntWritable sSize = new IntWritable();
        private IntWritable tSize = new IntWritable();

        //Four Optimization Variables
        private String rangeFirst_S = ""; //range value is inclusion => use <= for comparison
        private String rangeSecond_S = "";
        private String rangeFirst_T = "";
        private String rangeSecond_T = "";

        //convert map to MapWritable
//        private MapWritable toMapWritable(HashMap<String, String> map){
//            MapWritable result = new MapWritable();
//            if(map != null){
//                for(Map.Entry<String, String> entry : map.entrySet()){
//                    result.put(new Text(entry.getKey()), new Text(entry.getValue()));
//                }
//            }
//            return result;
//        }

        //private HashMap<String, ArrayList<Integer>> lookupTable = new HashMap<String, ArrayList<Integer>>();

        //the setup function will be called only once to populate the lookupTable
//        public void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException {
//            try {
//                Path path = new Path("hdfs://PartitionJoin/lookupTable.txt");
//                FileSystem fs = FileSystem.get(new Configuration());
//                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
//
//                try {
//                    String line;
//                    line = br.readLine();
//                    while (line != null) {
//                        String[] parts = line.split("-");
//                        String[] num = parts[1].split(" ");
//                        ArrayList numList = new ArrayList();
//                        for (String x : num) {
//                            numList.add(Integer.parseInt(x));
//                        }
//                        lookupTable.put(new String(parts[0]), new ArrayList(numList));
//                        line = br.readLine();
//                    }
//                } catch (IOException e) {
//                    System.out.println("Exception while populating lookup table");
//                    e.printStackTrace();
//                } finally {
//                    br.close();
//                }
//            } catch (Exception e) {
//                System.out.println("Exception while reading the lookup table");
//                e.printStackTrace();
//            }
//        }

        //An ArrayWritable class
//        public class IntArrayWritable extends ArrayWritable{
//            public IntArrayWritable(){
//                super(IntWritable.class);
//            }
//        }

        @Override
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
            String[] tableSizes = context.getConfiguration().get("tableSizes").split(" ");

            if(tableSizes[0] == null){
                System.out.println("NULL!");
            }
            else{
                sSize.set(Integer.parseInt(tableSizes[0]));
            }

            if(tableSizes[1] == null){
                System.out.println("NULL!");
            }
            else{
                tSize.set(Integer.parseInt(tableSizes[1]));
            }

            //HashMap<String, String> map = new HashMap<String, String>();

            int s_size=sSize.get();
            int t_size=tSize.get();

            //get the square length
            double squareLength = Math.sqrt(s_size*t_size/4);
            int s_squareLength=0;
            int t_squareLength=0;
            int c_S = 0;
            int c_T = 0;

            if(s_size==t_size){
                //for 4 reducers
                s_squareLength=s_size/2;
                t_squareLength=t_size/2;
            }else {
                double divisor=Math.sqrt(s_size*t_size/4.0);
                //for 4 reducers, always divided by 2
                s_squareLength = s_size/2;
                t_squareLength = t_size/2;
                c_S = (int)(s_size/divisor);
                c_T=(int)(t_size/divisor);
            }

            int i = 0;
//            for(; i < s_squareLength; i++){
//                String line = "S";
//                line = line+Integer.toString(i);
//                String reducers = "1 2";
//                lookupTable.put(line, reducers);
//            }
            rangeFirst_S = Integer.toString(s_squareLength) + " 1,2";

//            for(; i < s_size; i++){
//                String line = "S";
//                line = line+Integer.toString(i);
//                String reducers = "3 4";
//                lookupTable.put(line, reducers);
//            }
            rangeFirst_S = Integer.toString(s_size) + " 3,4";

            //for T column
            i=0;
//            for(; i < t_squareLength; i++){
//                String line = "T";
//                line = line+Integer.toString(i);
//                String reducers = "1 3";
//                lookupTable.put(line, reducers);
//            }
            rangeFirst_T = Integer.toString(t_squareLength) + " 1,3";

//            for(; i < t_size; i++){
//                String line = "T";
//                line = line+Integer.toString(i);
//                String reducers = "2 4";
//                lookupTable.put(line, reducers);
//            }
            rangeSecond_T = Integer.toString(t_size) + " 2,4";

            //populate the mapWritable
            //lookupTable = toMapWritable(map);
        }

        //map function that assigns each record to the assigned reducers based on the lookup table
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

//            HashMap<String, ArrayList<Integer>> lookupTable = new HashMap<String, ArrayList<Integer>>();
//            for(int i = 0; i <5; i++){
//                String line = new String("S");
//                line = line+Integer.toString(i);
//                ArrayList<Integer> reducers = new ArrayList<Integer>();
//                reducers.add(0);
//                reducers.add(1);
//                lookupTable.put(line, reducers);
//            }
//
//            for(int i = 5; i < 10; i++){
//                String line = new String("S");
//                line = line+Integer.toString(i);
//                ArrayList<Integer> reducers = new ArrayList<Integer>();
//                reducers.add(2);
//                reducers.add(3);
//                lookupTable.put(line, reducers);
//            }
//
//            //for T column
//            for(int i = 0; i <5; i++){
//                String line = new String("T");
//                line = line+Integer.toString(i);
//                ArrayList<Integer> reducers = new ArrayList<Integer>();
//                reducers.add(0);
//                reducers.add(2);
//                lookupTable.put(line, reducers);
//            }
//
//            for(int i = 5; i < 10; i++){
//                String line = new String("T");
//                line = line+Integer.toString(i);
//                ArrayList<Integer> reducers = new ArrayList<Integer>();
//                reducers.add(1);
//                reducers.add(3);
//                lookupTable.put(line, reducers);
//            }

            Text record = new Text();
            String line = value.toString();

            //randomization codes
            //int i = line.indexOf(" ");

            //split up the string from the tag and the rest of the key and values
            int i = line.indexOf(" ");
            String tag = line.substring(0, i);
            //line = (line.substring(i+1));
            record.set(line);

            String[] reducersArray = null;

            //simulation of the randomized algorithm
            if(tag.contains("S")){
                Random rand = new Random();
                int randInt = rand.nextInt(sSize.get());
                tag = tag + Integer.toString(randInt);

                //check to see the range falls into the first range or the second range
                if(randInt < Integer.parseInt(rangeFirst_S.substring(0,rangeFirst_S.indexOf(" ")))){
                    //falls into the first range
                    reducersArray = rangeFirst_S.substring(rangeFirst_S.indexOf(" ")+1).split(",");
                }
                else{
                    reducersArray = rangeSecond_S.substring(rangeSecond_S.indexOf(" ")+1).split(",");
                }
            }
            else if(tag.contains("T")){
                Random rand = new Random();
                int randInt = rand.nextInt(tSize.get());
                tag = tag + Integer.toString(randInt);

                //check to see the range falls into the first range or the second range
                if(randInt < Integer.parseInt(rangeFirst_T.substring(0,rangeFirst_T.indexOf(" ")))){
                    //falls into the first range
                    reducersArray = rangeFirst_T.substring(rangeFirst_T.indexOf(" ")+1).split(",");
                }
                else{
                    reducersArray = rangeSecond_T.substring(rangeSecond_T.indexOf(" ")+1).split(",");
                }
            }
            //look up in the lookup table and retrieve the reducer list
//            String tagKey = tag.toString();
//            Text tagKey = new Text();
//            tagKey.set(tag);

//            if(lookupTable.containsKey(tagKey)){
//                reducersArray = ((Text)lookupTable.get(tagKey)).toString().split(" ");
//            }
//            else{
//                //do nothing
//            }
//            if(lookupTable.containsKey(tag)){
//                reducersArray = lookupTable.get(tag).split(" ");
//            }
//            else{
//                //do nothing
//                context.write(new IntWritable(50), new Text("lookupTable does not contains key"));
//            }




            //output.collect(new IntWritable(1), reducersArray );
            //loop through the reducerArray and form key and value pair
            //and send each record to each reducer in the array
            if(reducersArray != null){
                for (String  reducerID : reducersArray) {
                    int j = Integer.parseInt(reducerID);
                    context.write(new IntWritable(j), record );
                    //test breaking point
                    //output.collect(new IntWritable(10), new Text("HelloWorld"));
                }
            }
            else{
                //set the int writable to 50 such that if the reducersArray is empty, then we can trace back from the logs
                context.write(new IntWritable(50), record);
            }

            //word.set(value);
//            Text record = new Text();
//            record.set(line);
            //context.write(new IntWritable(1), tagKey);
        }
    }

    public static class PartitionReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //two tables initialization to perform the joining
            ArrayList<String> tableS = new ArrayList<String>();
            ArrayList<String> tableT = new ArrayList<String>();
//
//            //group the records in the iterator into table S and table T
            for(Text val : values){
                String record = val.toString();
                int i = record.indexOf(" ");
                String tag = record.substring(0,i);
                String value = record.substring(i+1);

                if (tag.contentEquals("S")) {
                    tableS.add(value);
                } else if(tag.contentEquals("T")) {
                    tableT.add(value);
                }
                //context.write(new IntWritable(100), val);
            }
//
////            //perform the join using your preference of join algorithm
////            //pick either one table and hash it
            HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
            for (String s : tableS){
                int i = s.indexOf(" ");
                String keyValue = s.substring(0, i);
                //if the key doesn't exist
                if (!hm.containsKey(keyValue)) {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(s.substring(i+1));
                    hm.put(keyValue, list);
                } else {
                    //same key exists, then add it to the array list
                   hm.get(keyValue).add(s.substring(i+1));
                }
            }
//////
//////            //then iterate through the other table to produce the result
            for (String t : tableT) {
                //check to see if there's a match
                int i = t.indexOf(" ");
                String hashKey = t.substring(0, i);
                if (!hm.containsKey(hashKey)) {
                    //no match and don't do anything
                } else {
                    //match
                    for (String s : hm.get(hashKey)) {
                        String result = s + " " + hashKey + ":" + t;
                        context.write(key, new Text(result));
                    }
                }
            }
//            IntWritable i = new IntWritable(1);
//            context.write(i, new Text("Matched!"));
        }
    }

    public static void main(String[] args) throws Exception {
//        JobConf conf = new JobConf(PartitionJoin.class);
//        conf.setJobName("PartitionJoin");
//
//        conf.setOutputKeyClass(IntWritable.class);
//        conf.setOutputValueClass(Text.class);
//
//        conf.setMapperClass(PartitionMapper.class);
//        //conf.setCombinerClass(Reduce.class);
//        conf.setReducerClass(Reduce.class);
//        conf.setNumReduceTasks(4);
//
//        conf.setInputFormat(TextInputFormat.class);
//        conf.setOutputFormat(TextOutputFormat.class);
//
//        FileInputFormat.setInputPaths(conf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//
//        JobClient.runJob(conf);

        //After switching to a new version of mapreduce, 2.3.0
        //below is the new codes for setting up the job configuration
//        Configuration conf = new Configuration();
//        conf.set("sSize", args[0]);
//        conf.set("tSize", args[1]);
//        Job job = Job.getInstance(conf, "PartitionJoin");
//        job.setJarByClass(PartitionJoin.class);
//        job.setMapperClass(PartitionMapper.class);
//        job.setReducerClass(PartitionReducer.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(4);
//
//
//        FileInputFormat.addInputPath(job, new Path(args[2]));
//        FileOutputFormat.setOutputPath(job, new Path(args[3]));
//
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
        int res = ToolRunner.run(new Configuration(), new PartitionJoin(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception{

        Configuration conf = this.getConf();

        //Create job
        //Job job = new Job(conf, "Partition Join");
        Job job = Job.getInstance(conf, "PartitionJoin");
        job.setJarByClass(PartitionJoin.class);

        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}