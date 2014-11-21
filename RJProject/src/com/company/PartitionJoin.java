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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

//We can now begin tetsing

public class PartitionJoin {

    public static class PartitionMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        //initiate a look up table accessible for all
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

//        public void setup(org.apache.hadoop.mapreduce.Mapper.Context context){
//            for(int i = 0; i <5; i++){
//                String line = new String("S");
//                line = line+Integer.toString(i);
//                ArrayList<Integer> reducers = new ArrayList<Integer>();
//                reducers.add(1);
//                reducers.add(2);
//                lookupTable.put(line, reducers);
//            }
//
//            for(int i = 5; i < 10; i++){
//                String line = new String("S");
//                line = line+Integer.toString(i);
//                ArrayList<Integer> reducers = new ArrayList<Integer>();
//                reducers.add(3);
//                reducers.add(4);
//                lookupTable.put(line, reducers);
//            }
//
//            //for T column
//            for(int i = 0; i <5; i++){
//                String line = new String("T");
//                line = line+Integer.toString(i);
//                ArrayList<Integer> reducers = new ArrayList<Integer>();
//                reducers.add(1);
//                reducers.add(3);
//                lookupTable.put(line, reducers);
//            }
//
//            for(int i = 5; i < 10; i++){
//                String line = new String("T");
//                line = line+Integer.toString(i);
//                ArrayList<Integer> reducers = new ArrayList<Integer>();
//                reducers.add(2);
//                reducers.add(4);
//                lookupTable.put(line, reducers);
//            }
//        }

        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            HashMap<String, ArrayList<Integer>> lookupTable = new HashMap<String, ArrayList<Integer>>();
            for(int i = 0; i <5; i++){
                String line = new String("S");
                line = line+Integer.toString(i);
                ArrayList<Integer> reducers = new ArrayList<Integer>();
                reducers.add(0);
                reducers.add(1);
                lookupTable.put(line, reducers);
            }

            for(int i = 5; i < 10; i++){
                String line = new String("S");
                line = line+Integer.toString(i);
                ArrayList<Integer> reducers = new ArrayList<Integer>();
                reducers.add(2);
                reducers.add(3);
                lookupTable.put(line, reducers);
            }

            //for T column
            for(int i = 0; i <5; i++){
                String line = new String("T");
                line = line+Integer.toString(i);
                ArrayList<Integer> reducers = new ArrayList<Integer>();
                reducers.add(0);
                reducers.add(2);
                lookupTable.put(line, reducers);
            }

            for(int i = 5; i < 10; i++){
                String line = new String("T");
                line = line+Integer.toString(i);
                ArrayList<Integer> reducers = new ArrayList<Integer>();
                reducers.add(1);
                reducers.add(3);
                lookupTable.put(line, reducers);
            }

            Text record = new Text();
            String line = value.toString();

            //split up the string from the tag and the rest of the key and values
            int i = line.indexOf(" ");
            String tag = line.substring(0, i);
            line = (line.substring(i+1));
            record.set(line);

            //look up in the lookup table and retrieve the reducer list
//            String tagKey = tag.toString();
            ArrayList<Integer> reducersArray = lookupTable.get(tag);

            //loop through the reducerArray and form key and value pair
            //and send each record to each reducer in the array
            for (int k = 0; k < reducersArray.size(); k++) {
                int j = reducersArray.get(k);
                output.collect(new IntWritable(j), record);
                //test breaking point
                //output.collect(new IntWritable(10), new Text("HelloWorld"));
            }
            //word.set(value);
//            Text record = new Text();
//            record.set(line);
            //output.collect(new IntWritable(1), record);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            //two tables initialization to perform the joining
            List<String> tableS = new ArrayList<String>();
            List<String> tableT = new ArrayList<String>();

            //group the records in the iterator into table S and table T
            while (values.hasNext()) {
                String record = values.next().toString();
                int i = record.indexOf(' ');
                String tag = record.substring(0, i);
                String value = record.substring(i+1);

                if (tag.contentEquals("S")) {
                    tableS.add(value);
                } else {
                    tableT.add(value);
                }
            }

            //perform the join using your preference of join algorithm
            //pick either one table and hash it
            HashMap<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
            for (String s : tableS) {
                //if the key doesn't exist
                if (hm.get(s.substring(0, s.indexOf(' '))) == null) {
                    ArrayList<String> list = new ArrayList<String>();
                    list.add(s.substring(s.indexOf(' ')));
                    hm.put(s.substring(0, s.indexOf(' ')), list);
                } else {
                    //same key exists, then add it to the array list
                    hm.get(s.substring(0, s.indexOf(' '))).add(s.substring(s.indexOf(' ')));
                }
            }

            //then iterate through the other table to produce the result
            for (String t : tableT) {
                //check to see if there's a match
                String hashKey = t.substring(0, t.indexOf(' '));
                if (hm.get(hashKey) != null) {
                    //match
                    for (String s : hm.get(hashKey)) {
                        String result = s + " " + hashKey + ":" + t;
                        output.collect(key, new Text(result));
                    }
                } else {
                    //no match and don't do anything
                }
            }

//            while(values.hasNext()){
//               //key1=new IntWritable(key.get());
//                //output.collect(key1,new Text("Hello world"));
//                //System.out.println("Key is:" + key1);
//                output.collect(key, values.next());
//            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(PartitionJoin.class);
        conf.setJobName("PartitionJoin");

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(PartitionMapper.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setNumReduceTasks(4);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}