package com.company;

import java.util.ArrayList;

/**
 * Created by Alex on 12/6/14.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        //The code below is pulled from my testing project. So change accordingly.
        //Assume |S|<|T|
        //int numReduers = Integer.parseInt(args[0]);
        //int sSize = Integer.parseInt(args[1]);
        //int tSize = Integer.parseInt(args[2]);
        double numReduers =5;
        double sSize=2;
        double tSize=13;

        double idealSquareLength = Math.sqrt((sSize*tSize)/numReduers);
        double SquareArea = (sSize*tSize)/numReduers;

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

        else if (sSize < tSize/numReduers)
        {
            //Extreme case : since I take floor, so put the left area into last region. It is not optimal. We will improve it later.
            // get the range increment. USE FLOOR
            double rangeIncrement=Math.floor(tSize/numReduers);
            // divide into two cases.

            //partition S into one row
            ArrayList<Integer> rowReducersID = new ArrayList<Integer>();
            for(int i = 1; i <= numReduers; i++){
                rowReducersID.add(i);
            }
            Region SRegion = new Region(new Range(0, (int)sSize), "S", rowReducersID);

            //partition T into |T|/r
            // assign range, what table it belong to and reducer Id to each region
            ArrayList<Region> TRegions =new ArrayList<Region>();
            for (int i=1; i<=numReduers-1;i++)
            {
                Region temp = new Region(new Range((int)((i-1)*rangeIncrement),(int)(i*rangeIncrement)), "T", i);
                TRegions.add(temp);
            }
            Region temp1=new Region(new Range((int)((numReduers-1)*rangeIncrement),(int)tSize),"T",(int)numReduers);
            TRegions.add(temp1);
            System.out.println("Done");
        }
        else
        {
            //General case
            System.out.println("Done");
        }
    }
}
