package com.company;

import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by syq3b on 11/17/2014.
 */
public class GenerateLookupTable {
    public static void main(String[] args)throws IOException{
//        int sizeOfS = Integer.parseInt(args[0]);
//        int sizeOfT = Integer.parseInt(args[1]);
        PrintWriter writer = new PrintWriter("lookupTable.txt", "UTF-8");

        //for S row
        for(int i = 0; i <5; i++){
            String line = new String("S");
            line = line+Integer.toString(i)+"-1 2";
            writer.println(line);
        }

        for(int i = 5; i < 10; i++){
            String line = new String("S");
            line = line+Integer.toString(i)+"-3 4";
            writer.println(line);
        }

        //for T column
        for(int i = 0; i <5; i++){
            String line = new String("T");
            line = line+Integer.toString(i)+"-1 3";
            writer.println(line);
        }

        for(int i = 5; i < 10; i++){
            String line = new String("T");
            line = line+Integer.toString(i)+"-2 4";
            writer.println(line);
        }
        writer.close();

    }
}
