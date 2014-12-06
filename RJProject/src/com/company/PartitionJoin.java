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
        private boolean isHorizontal=false;
        private boolean isVertical=false;

        private int limit1=0;
        private int limit2=0;
        private int limit3=0;

        //Four Optimization Variables
        //Specific for 4 reducers Case
        private static String rangeFirst_S[] = {"1","2"};
        private static String rangeSecond_S[] = {"3","4"};
        private static String rangeFirst_T[] = {"1","3"};
        private static String rangeSecond_T[] = {"2","4"};

        //The Map function will based on the partitioning schema to assign each tuple to destined reducers to reach
        //balance workload and reduce data skew
        private static String all_reducers[]={"1","2","3","4"};
        private static String reducer_one[]={"1"};
        private static String reducer_two[]={"2"};
        private static String reducer_three[]={"3"};
        private static String reducer_four[]={"4"};

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
            int s_squareLength=0;
            int t_squareLength=0;

            if(s_size==t_size){
                //for 4 reducers
                //s_squareLength=s_size/2;
                //t_squareLength=t_size/2;
                limit1=s_size/2;
                limit2=t_size/2;

            }else if( s_size<= t_size/4.0){
                isVertical=true;
                int temp = (int) (t_size/4.0);
                limit1=temp;
                limit2=temp*2;
                limit3=temp*3;

            }else if (t_size<=s_size/4.0){
                isHorizontal=true;
                int temp = (int) (s_size/4.0);
                limit1=temp;
                limit2=temp*2;
                limit3=temp*3;
            }else {
//                double divisor=Math.sqrt(s_size*t_size/4.0);
//                //for 4 reducers, always divided by 2
//                s_squareLength = s_size/2;
//                t_squareLength = t_size/2;
//                c_S = (int)(s_size/divisor);
//                c_T=(int)(t_size/divisor);

                limit1 = s_size/2;
                limit2 = t_size/2;

            }

            //This is the Optimization Step where we avoid the use of HashTable and just use variables to represent
            //the range of each partition
//            rangeFirst_S = Integer.toString(s_squareLength) + " 1,2";
//            rangeSecond_S = Integer.toString(s_size) + " 3,4";
//            rangeFirst_T = Integer.toString(t_squareLength) + " 1,3";
//            rangeSecond_T = Integer.toString(t_size) + " 2,4";

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

        //A test function for the attempting implementation
        /*
        private void test()
        {
            //The code below is pulled from my testing project. So change accordingly.
            //Assume |S|<|T|
            int numReduers = Integer.parseInt(args[0]);
            int sSize = Integer.parseInt(args[1]);
            int tSize = Integer.parseInt(args[2]);

            double idealSquareLength = Math.sqrt(((double)sSize*tSize)/numReduers);
            double SquareArea = ((double)sSize*tSize)/numReduers;

            //calculate the Cs and Ct
            float sCoefficient = (float)(sSize/idealSquareLength);
            float tCoefficient = (float)(tSize/idealSquareLength);

            //This is for storing all of the regions as a result from the parititoning
            ArrayList<Region> reducersRegion = new ArrayList<Region>();

            //Begin checking for cases
            //three cases: ideal case, extreme case, general case
            //Ideal case: idealSquareLength and SquareArea should be integers. Also |S| and |T| are multiples of idealSquareLength
            if (Math.floor(idealSquareLength)==idealSquareLength && sCoefficient%1 == 0 && tCoefficient%1 == 0)
             {
                //Ideal Case

             }
            else if (sSize<tSize/numReduers)
            {
                //Extreme case
                // get the range increment and round it. DO NOT USE FLOOR OR CEILING
                int rangeIncrement=Math.round((float)(tSize/numReduers));
                // divide into two cases.
                if (rangeIncrement*numReduers>sSize*tSize)
                {
                    //partition S into one row
                    ArrayList<Integer> rowReducersID = new ArrayList<Integer>();
                    for(int i = 1; i <= numReduers; i++){
                        rowReducersID.add(i);
                    }
                    Region rowRegion = new Region(new Range(0, sSize), "S", rowReducersID);

                    //partition T into |T|/r
                    // assign range, what table it belong to and reducer Id to each region
                    ArrayList<Region> Regions =new ArrayList<Region>();
                    for (int i=1; i<=numReduers-1;i++)
                    {
                        Region temp = new Region(new Range((i-1)*rangeIncrement,i*rangeIncrement), "T", i);
                        Regions.add(temp);
                    }
                    Region temp1=new Region(new Range((numReduers-1)*rangeIncrement,tSize),"T",numReduers);
                    Regions.add(temp1);
                }
                else
                {

                }

            }
            else
            {
                //General case
            }


            if(sCoefficient%1 == 0 && tCoefficient%1 == 0){
                System.out.println("Both Cs and Ct are integer multipliers");
            }
            else{
                //Then it belongs to case 2
                //In case 2, first check to see if the extreme case applied
                //Extreme case condition: |S| < the ideal suquare length (this implies that the side length of the optimal
                //  suare that matches the lower bounds is "taller" than the join matrix. Hence the lower bounds are not
                //  tight, because no partition of the matrix can have more than |S| tuples from input set S.
                //The optimal partitioning of the matrix into r regions would then consists of rectangles of size |S| by
                //  |T|/r. Vice Versa, |T| by |S|/r.
                if(sSize < idealSquareLength){
                    //Partition into |S| by |T|/r
                    //For row, only one region
                    ArrayList<Integer> rowReducersID = new ArrayList<Integer>();
                    for(int i = 1; i <= numReduers; i++){
                        rowReducersID.add(i);
                    }
                    Region rowRegion = new Region(new Range(0, sSize), "S", rowReducersID);
                }
                else if (tSize < idealSquareLength){
                    //Partition into |T| by |S|/r
                }
            }
            System.out.println("Done");

        }
         */
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

                if(isHorizontal){

                    if (randInt<limit1){
                        reducersArray=reducer_one;
                    }else if (randInt<limit2){
                        reducersArray=reducer_two;

                    }else if (randInt<limit3){
                        reducersArray=reducer_three;
                    }else{
                        reducersArray=reducer_four;
                    }

                }else if (isVertical){
                        reducersArray=all_reducers;
                }
                //check to see the range falls into the first range or the second range
                else if(randInt<limit1){
                   reducersArray=rangeFirst_S;
                }else{
                    reducersArray=rangeSecond_S;

                }

//                else if(randInt < Integer.parseInt(rangeFirst_S.substring(0,rangeFirst_S.indexOf(" ")))){
//                    //falls into the first range
//                    reducersArray = rangeFirst_S.substring(rangeFirst_S.indexOf(" ")+1).split(",");
//                }
//                else{
//                    reducersArray = rangeSecond_S.substring(rangeSecond_S.indexOf(" ")+1).split(",");
//                }
            }
            else{
                Random rand = new Random();
                int randInt = rand.nextInt(tSize.get());
                //tag = tag + Integer.toString(randInt);
                if(isVertical){

                    if (randInt<limit1){
                        reducersArray=reducer_one;
                    }else if (randInt<limit2){
                        reducersArray=reducer_two;

                    }else if (randInt<limit3){
                        reducersArray=reducer_three;
                    }else{
                        reducersArray=reducer_four;
                    }

                }else if (isHorizontal){
                    reducersArray=all_reducers;
                }else if (randInt<limit2){
                    reducersArray=rangeFirst_T;
                }else {
                    reducersArray=rangeSecond_T;
                }
                //check to see the range falls into the first range or the second range
//                else if(randInt < Integer.parseInt(rangeFirst_T.substring(0,rangeFirst_T.indexOf(" ")))){
//                    //falls into the first range
//                    reducersArray = rangeFirst_T.substring(rangeFirst_T.indexOf(" ")+1).split(",");
//                }
//                else{
//                    reducersArray = rangeSecond_T.substring(rangeSecond_T.indexOf(" ")+1).split(",");
//                }
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

            //perform the join using your preference of join algorithm
            //pick either one table and hash it
            HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();

            //ArrayList<String> tableS = new ArrayList<String>(); Trying to reduce memory
            ArrayList<String> tableT = new ArrayList<String>();

            //group the records in the iterator into table S and table T
            for(Text val : values){
                String record = val.toString();
                int i = record.indexOf(" ");
                String tag = record.substring(0,i);
                String value = record.substring(i+1);

                if (tag.contentEquals("S")) {
                    //get the key-value information
                    int j = value.indexOf(" ");
                    String tagKey = value.substring(0,j);
                    String mapValue = value.substring(j+1);

                    //if the key doesn't exist
                    if (!hm.containsKey(tagKey)) {
                        ArrayList<String> list = new ArrayList<String>();
                        list.add(mapValue);
                        hm.put(tagKey, list);
                    } else {
                        //same key exists, then add it to the array list
                        hm.get(tagKey).add(mapValue);
                    }
                } else if(tag.contentEquals("T")) {
                    tableT.add(value);
                }
            }

//            for (String s : tableS){
//                int i = s.indexOf(" ");
//                String keyValue = s.substring(0, i);
//                //if the key doesn't exist
//                if (!hm.containsKey(keyValue)) {
//                    ArrayList<String> list = new ArrayList<String>();
//                    list.add(s.substring(i+1));
//                    hm.put(keyValue, list);
//                } else {
//                    //same key exists, then add it to the array list
//                   hm.get(keyValue).add(s.substring(i+1));
//                }
//            }

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

        System.out.println("Starting mapreduce parogram.......");

        long time = System.currentTimeMillis();

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
        boolean result = job.waitForCompletion(true);

        System.out.println("Total execution time:"+(System.currentTimeMillis()-time));

        return result ? 0 : 1;

        //return job.waitForCompletion(true) ? 0 : 1;
    }
}