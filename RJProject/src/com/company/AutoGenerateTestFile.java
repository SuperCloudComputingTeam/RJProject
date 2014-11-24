package com.company;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * Created by syq3b on 11/17/2014.
 */
public class AutoGenerateTestFile {
    public static void main(String[] args)throws IOException{
        PrintWriter writer = new PrintWriter("T.txt", "UTF-8");

        for(int i = 0; i < 10000000; i++){
            String line = new String("T");

            //Represent the lookup table index
            line = line + Integer.toString(i);

            //Represent the table tag
            line = line + " " + "T";

            //generating random number from 0 to 100
            //general formula = random(max-min+1)+min
            Random rand = new Random();
            int randInt = rand.nextInt(10)+1;
            line = line + " " + Integer.toString(randInt);

            int randAge = rand.nextInt(16)+15;
            line = line + " " + Integer.toString(randAge);

            writer.println(line);
        }
        writer.close();

        writer = new PrintWriter("S.txt", "UTF-8");

        for(int i = 0; i < 10000000; i++){
            String line = new String("S");

            //Represent the lookup table index
            line = line + Integer.toString(i);

            //Represent the table tag
            line = line + " " + "S";

            //generating random number from 0 to 100
            //general formula = random(max-min+1)+min
            Random rand = new Random();
            int randInt = rand.nextInt(10)+1;
            line = line + " " + Integer.toString(randInt);

            int randAge = rand.nextInt(16)+15;
            line = line + " " + Integer.toString(randAge);

            writer.println(line);
        }
        writer.close();

    }
}
