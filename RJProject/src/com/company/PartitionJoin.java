package com.company;

/**
 * Created by San on 11/10/2014.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class PartitionJoin extends Configured implements Tool{

    public static class PartitionMapper extends Mapper<Object, Text, IntWritable, Text> {

        //Initiate a look up table accessible for all - For Without Optimization Purposes
        //private static HashMap<String, String> lookupTable = new HashMap<String, String>();

        //size information for S and T table
        private IntWritable sSize = new IntWritable();
        private IntWritable tSize = new IntWritable();

        //Four Optimization Variables
        //Specific for 4 reducers Case
        private String rangeFirst_S = "";
        private String rangeSecond_S = "";
        private String rangeFirst_T = "";
        private String rangeSecond_T = "";

        //Can be used if needs to pass HashMap-like object among map-reduce tasks
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

        //An ArrayWritable class
//        public class IntArrayWritable extends ArrayWritable{
//            public IntArrayWritable(){
//                super(IntWritable.class);
//            }
//        }

        //The setup function will be called once for each mapper, and it initializes variables that can  be available for all map tasks
        @Override
        protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
            //Retrieving the relation sizes information from the user command line input
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


            int s_size=sSize.get();
            int t_size=tSize.get();

            int c_S=0;
            int c_T=0;

            //********************************** paper implementation of partitioning *****************

            //get the square length
            int s_squareLength;
            int t_squareLength;

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

            //This is the Optimization Step where we avoid the use of HashTable and just use variables to represent
            //the range of each partition
            rangeFirst_S = Integer.toString(s_squareLength) + " 1,2";
            rangeSecond_S = Integer.toString(s_size) + " 3,4";
            rangeFirst_T = Integer.toString(t_squareLength) + " 1,3";
            rangeSecond_T = Integer.toString(t_size) + " 2,4";

            //The below portion is for using HashMap to represent the lookupTable
            //De-comment when need to use it for performing testing and comparison
            //HashMap<String, String> map = new HashMap<String, String>();
//            int i = 0;
//            for(; i < s_squareLength; i++){
//                String line = "S";
//                line = line+Integer.toString(i);
//                String reducers = "1 2";
//                lookupTable.put(line, reducers);
//            }

//            for(; i < s_size; i++){
//                String line = "S";
//                line = line+Integer.toString(i);
//                String reducers = "3 4";
//                lookupTable.put(line, reducers);
//            }

            //for T column
//            i=0;
//            for(; i < t_squareLength; i++){
//                String line = "T";
//                line = line+Integer.toString(i);
//                String reducers = "1 3";
//                lookupTable.put(line, reducers);
//            }

//            for(; i < t_size; i++){
//                String line = "T";
//                line = line+Integer.toString(i);
//                String reducers = "2 4";
//                lookupTable.put(line, reducers);
//            }

            //************************************horizontal partitioning******************************************

            /*
            c_S=s_size/4;
            for(;i<c_S;i++){
                String line = "S";
                line = line+Integer.toString(i);
                String reducers = "1";
                map.put(line, reducers);
            }
            int temp = c_S*2;

            for(;i< temp ;i++){
                String line = "S";
                line = line+Integer.toString(i);
                String reducers = "2";
                map.put(line, reducers);
            }

            temp=c_S*3;
            for(;i< temp ;i++){
                String line = "S";
                line = line+Integer.toString(i);
                String reducers = "3";
                map.put(line, reducers);
            }

            for(;i< s_size ;i++){
                String line = "S";
                line = line+Integer.toString(i);
                String reducers = "4";
                map.put(line, reducers);
            }


            i=0;
            for(; i < t_size; i++){
                String line = "T";
                line = line+Integer.toString(i);
                String reducers = "1 2 3 4";
                map.put(line, reducers);
            }

            */


            // **********************************vertical partitioning*********************************************


//            i=0;
//            for(; i < s_size; i++){
//                String line = "S";
//                line = line+Integer.toString(i);
//                String reducers = "1 2 3 4";
//                map.put(line, reducers);
//            }
//
//            i=0;
//            c_T=t_size/4;
//            for(;i<c_T;i++){
//                String line = "T";
//                line = line+Integer.toString(i);
//                String reducers = "1";
//                map.put(line, reducers);
//            }
//            int temp = c_T*2;
//
//            for(;i< temp ;i++){
//                String line = "T";
//                line = line+Integer.toString(i);
//                String reducers = "2";
//                map.put(line, reducers);
//            }
//
//            temp=c_T*3;
//            for(;i< temp ;i++){
//                String line = "T";
//                line = line+Integer.toString(i);
//                String reducers = "3";
//                map.put(line, reducers);
//            }
//
//            for(;i< t_size ;i++){
//                String line = "S";
//                line = line+Integer.toString(i);
//                String reducers = "4";
//                map.put(line, reducers);
//            }

            //populate the mapWritable
            //lookupTable = toMapWritable(map);
        }

        //The Map function will based on the partitioning schema to assign each tuple to destined reducers to reach
        //balance workload and reduce data skew
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text record = new Text();
            String line = value.toString();

            int i = line.indexOf(" ");
            String tag = line.substring(0, i); //get the tag information (table origin info)
            record.set(line); //set the record to be the line information

            String[] reducersArray = null; //store the destined reducers ids info

            if(tag.contains("S")){
                Random rand = new Random();
                //Simulation of the randomization process
                int randInt = rand.nextInt(sSize.get()); //generate a random number from 0 to the size of S
                //tag = tag + Integer.toString(randInt);

                //check to see the range falls into the first range or the second range
                if(randInt < Integer.parseInt(rangeFirst_S.substring(0,rangeFirst_S.indexOf(" ")))){
                    //falls into the first range
                    reducersArray = rangeFirst_S.substring(rangeFirst_S.indexOf(" ")+1).split(",");
                }
                else{
                    reducersArray = rangeSecond_S.substring(rangeSecond_S.indexOf(" ")+1).split(",");
                }
            }
            else{
                Random rand = new Random();
                int randInt = rand.nextInt(tSize.get());
                //tag = tag + Integer.toString(randInt);

                //check to see the range falls into the first range or the second range
                if(randInt < Integer.parseInt(rangeFirst_T.substring(0,rangeFirst_T.indexOf(" ")))){
                    //falls into the first range
                    reducersArray = rangeFirst_T.substring(rangeFirst_T.indexOf(" ")+1).split(",");
                }
                else{
                    reducersArray = rangeSecond_T.substring(rangeSecond_T.indexOf(" ")+1).split(",");
                }
            }

            //------------------This portion of codes use the lookupTable-----//
            //look up in the lookup table and retrieve the reducer list
            /**********************************************************
            String tagKey = tag.toString();
            Text tagKey = new Text();
            tagKey.set(tag);

            if(lookupTable.containsKey(tagKey)){
                reducersArray = ((Text)lookupTable.get(tagKey)).toString().split(" ");
            }
            else{
                //do nothing
            }
            if(lookupTable.containsKey(tag)){
                reducersArray = lookupTable.get(tag).split(" ");
            }
            else{
                //do nothing
                context.write(new IntWritable(50), new Text("lookupTable does not contains key"));
            }
            ************************************************************/

            //Loop through the reducerArray and form key and value pair
            //and send each record to each reducer in the array
            if(reducersArray != null){
                for (String  reducerID : reducersArray) {
                    int j = Integer.parseInt(reducerID);
                    context.write(new IntWritable(j), record );
                }
            }
            else{
                //set the int writable to 50 such that if the reducersArray is empty, then we can trace back from the logs
                context.write(new IntWritable(50), record);
            }
        }
    }

    //Perform join using desired joining algorithm
    public static class PartitionReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //two tables initialization to perform the joining
            ArrayList<String> tableS = new ArrayList<String>();
            ArrayList<String> tableT = new ArrayList<String>();

            //group the records in the iterator into table S and table T
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
            }

            //perform the join using your preference of join algorithm
            //pick either one table and hash it
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

            //then iterate through the other table to produce the result
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
        }
    }

    //The main method calls the ToolRunner.run method to handle custom command line input
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PartitionJoin(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception{
        Configuration conf = this.getConf();

        //Create job
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

        return job.waitForCompletion(true) ? 0 : 1;
    }
}