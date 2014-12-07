package com.company;

import java.util.ArrayList;

/**
 * Created by Alex on 12/6/14.
 */
public class Test
{
    public ArrayList<ReducerRegion> Reducers = new ArrayList<ReducerRegion>();

    public  void Test(double S, double T, double r) throws Exception {
        //Assume |S|<|T|
        //int numReduers = Integer.parseInt(args[0]);
        //int sSize = Integer.parseInt(args[1]);
        //int tSize = Integer.parseInt(args[2]);

        double numReduers =r;
        double sSize=S;
        double tSize=T;

        numReduers=5;
        sSize=5;
        tSize=10;

        double idealSquareLength = Math.sqrt((sSize*tSize)/numReduers);
        double idealSquareArea = (sSize*tSize)/numReduers;

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
            System.out.println("Ideal Case");

        }

        else if (sSize < tSize/numReduers)
        {
            System.out.println("Extreme Case");
            //Extreme case : since I take floor, so put the left area into last region. It is not optimal. We will improve it later.
            // get the range increment. USE FLOOR
            double rangeIncrement=Math.floor(tSize / numReduers);
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

        }
        else
        {
            //Generic case
            System.out.println("Generic Case");
            // two cases. One is more than one reducer left, the other is only one reducer left(8*13)
            //***********************************************CASE 1**********************************
            double lowBoundLength = Math.floor(idealSquareLength);
            double highBoundLength = Math.ceil(idealSquareLength);
            int s_regionLength = -1;
            int t_regionLength = -1;
            int s_leftLength = -1;
            int t_leftLength = -1;
            int numLeftReducers = -1;

            double floorDiff = Math.abs(Math.pow(lowBoundLength,2)-idealSquareArea);
            double mediDiff = Math.abs(lowBoundLength*highBoundLength-idealSquareArea);
            double ceilDiff = Math.abs(Math.pow(highBoundLength,2)-idealSquareArea);

            //find length of each side of region(s_regionLength and t_regionLength)
            if (Math.min(Math.min(floorDiff,mediDiff),ceilDiff) == floorDiff)
            {
                s_regionLength = (int)lowBoundLength;
                t_regionLength = (int)lowBoundLength;
            }
            else if (Math.min(Math.min(floorDiff,mediDiff),ceilDiff) == mediDiff)
            {
                s_regionLength = (int)lowBoundLength;
                t_regionLength = (int)highBoundLength;
            }
            else
            {
                s_regionLength =(int)highBoundLength;
                t_regionLength = (int)highBoundLength;
            }
            numLeftReducers = (int)(numReduers-Math.floor(sCoefficient)*Math.floor(tCoefficient));
            s_leftLength = (int)(sSize-Math.floor(sCoefficient) * s_regionLength);
            t_leftLength = (int)(tSize-Math.floor(tCoefficient) * t_regionLength);

            //Partition left area
            double s_newRegionLength;
            double t_newRegionLength;
            double r_newRegion;
            if (numLeftReducers==2)
            {
                Range s =new Range((int)(tSize-t_leftLength), (int)tSize);
                //Range t =new Range(0,)
                ReducerRegion temp = new ReducerRegion();

                return;
            }
            else if(numLeftReducers==1)
            {
                System.out.print("only one reducer left, this part doesn't finished yet");
                System.exit(0);
                return;
            }
            else
            {
                // find the optimal cut which partitions have minimum difference
                if (Math.min(Math.abs(s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)),Math.abs(t_leftLength*sSize-s_leftLength*(tSize-t_leftLength)))==Math.abs(s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)))
                {
                    //the bigger partition got more reducers
                    if (s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)>0)
                    {
                        this.Test(s_leftLength,(int)tSize,Math.ceil(numLeftReducers/2);
                        //check which side of region should be S or T parameter
                        if (t_leftLength<sSize-s_leftLength)
                        {
                            this.Test(t_leftLength,s_leftLength,numLeftReducers-Math.ceil(numLeftReducers/2);

                        }
                        else
                        {
                            this.Test(s_leftLength,t_leftLength,numLeftReducers-Math.ceil(numLeftReducers/2);
                        }
                    }
                    else
                    {
                        this.Test(s_leftLength,(int)tSize,numLeftReducers-Math.ceil(numLeftReducers/2);
                        if (t_leftLength<sSize-s_leftLength)
                        {
                            this.Test(t_leftLength,s_leftLength,Math.ceil(numLeftReducers/2);
                        }
                        else
                        {
                            this.Test(s_leftLength,t_leftLength,Math.ceil(numLeftReducers/2);
                        }
                    }
                }
                else//16 and 10
                {
                    if (t_leftLength*sSize-s_leftLength*(tSize-t_leftLength)>0)
                    {
                        this.Test(t_leftLength,sSize,Math.ceil(numLeftReducers/2);
                        if (s_leftLength<tSize-t_leftLength)
                        {
                            this.Test(s_leftLength,tSize-t_leftLength,numLeftReducers-Math.ceil(numLeftReducers/2);
                        }
                        else
                        {
                            this.Test(tSize-t_leftLength,s_leftLength,numLeftReducers-Math.ceil(numLeftReducers/2);
                        }

                    }
                    else
                    {
                        this.Test(t_leftLength,sSize,numLeftReducers-Math.ceil(numLeftReducers/2);
                        if (s_leftLength<tSize-t_leftLength)
                        {
                            this.Test(s_leftLength,tSize-t_leftLength,Math.ceil(numLeftReducers/2);
                        }
                        else
                        {
                            this.Test(tSize-t_leftLength,s_leftLength,Math.ceil(numLeftReducers/2);
                        }
                    }
                }
            }





        }
    }
}
