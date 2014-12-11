package com.company;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by azhar on 12/8/14.
 */
public class ReducerManager{

    public ArrayList<ReducerRegion> Reducers;
    public int id;
    //public int numReducer;
    //public HashMap<String, String> map;

    public ReducerManager(int total_reducers)
    {
        Reducers =new ArrayList<ReducerRegion>();
        id =1;
    }



    public void Partition (double S, double T, double numReduers, Range s_r, Range t_r, boolean fl)  {
        //Assume |S|<|T|
        //int numReduers = Integer.parseInt(args[0]);
        //int sSize = Integer.parseInt(args[1]);
        //int tSize = Integer.parseInt(args[2]);
        //ArrayList<ReducerRegion> Reducers =new ArrayList<ReducerRegion>();



        double sSize=S;
        double tSize=T;
        Range sRange=s_r;
        Range tRange =t_r;
        Range sRangeTemp;
        Range tRangeTemp;
        boolean flag =fl;


        double idealSquareLength = Math.sqrt((sSize*tSize)/numReduers);
        double idealSquareArea = (sSize*tSize)/numReduers;

        //calculate the Cs and Ct
        float sCoefficient = (float)(sSize/idealSquareLength);
        float tCoefficient = (float)(tSize/idealSquareLength);

        //This is for storing all of the regions as a result from the parititoning

        //Begin checking for cases
        //three cases: ideal case, extreme case, general case
        //Ideal case: idealSquareLength and SquareArea should be integers. Also |S| and |T| are multiples of idealSquareLength
        //*********************************************Ideal Case************************************************************
        if (Math.floor(idealSquareLength)==idealSquareLength && sCoefficient%1 == 0 && tCoefficient%1 == 0)
        {
            //Ideal Case
            System.out.println("Ideal Case");

            Range idealsRange;
            Range idealtRange;

            for (int i=0; i<sCoefficient;i++)
            {
                for (int j=0;j<tCoefficient;j++)
                {
                    idealsRange = new Range((int)(i*idealSquareLength),(int)((i+1)*idealSquareLength));
                    idealtRange = new Range((int)(j*idealSquareLength),(int)((j+1)*idealSquareLength));
                    ReducerRegion reducer = new ReducerRegion(idealsRange,idealtRange,id);
                    Reducers.add(reducer);
                    id++;
                }

            }

        }
        //**********************************************Extreme Case**********************************************************
        else if (sSize < tSize/numReduers)
        {
            System.out.println("Extreme Case");
            //Extreme case : since I take floor, so put the left area into last region. It is not optimal. We will improve it later.
            // get the range increment. USE FLOOR
            if (numReduers==1)
            {

                if (flag)
                {
                    ReducerRegion reducer =new ReducerRegion(sRange,tRange,id);
                    Reducers.add(reducer);
                    id++;
                }
                else
                {
                    ReducerRegion reducer =new ReducerRegion(tRange,sRange,id);
                    Reducers.add(reducer);
                    id++;
                }

            }
            else
            {
                double rangeIncrement=Math.floor(tSize / numReduers);
                Range tExtremeRange;
                //assign reducerId to each region
                if (flag)
                {
                    for (int i=1;i<=numReduers-1;i++  )
                    {
                        tExtremeRange =new Range(tRange.low+(i-1)*(int)rangeIncrement,tRange.high+(i-1)*(int)rangeIncrement);
                        ReducerRegion reducer =new ReducerRegion(sRange,tExtremeRange,id);
                        Reducers.add(reducer);
                        id++;
                    }
                    tExtremeRange = new Range((int)((numReduers-1)*rangeIncrement),tRange.high);
                    ReducerRegion reducer =new ReducerRegion(sRange,tExtremeRange,id);
                    Reducers.add(reducer);
                    id++;
                }
                else
                {
                    for (int i=1;i<=numReduers-1;i++  )
                    {
                        tExtremeRange =new Range(tRange.low+(i-1)*(int)rangeIncrement,tRange.high+(i-1)*(int)rangeIncrement);
                        ReducerRegion reducer =new ReducerRegion(tExtremeRange,sRange,id);
                        Reducers.add(reducer);
                        id++;
                    }
                    tExtremeRange = new Range((int)((numReduers-1)*rangeIncrement),tRange.high);
                    ReducerRegion reducer =new ReducerRegion(tExtremeRange,sRange,id);
                    Reducers.add(reducer);
                    id++;
                }

            }
            return; // go back to last recursion level
        }
        //****************************************Generic Case****************************************************
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
            int numUsereducers=-1;
            double sIndex;
            double tIndex;

            double floorDiff = Math.abs(Math.pow(lowBoundLength,2)-idealSquareArea);
            double mediDiff = Math.abs(lowBoundLength*highBoundLength-idealSquareArea);
            double ceilDiff = Math.abs(Math.pow(highBoundLength,2)-idealSquareArea);

            //find length of each side of region(s_regionLength and t_regionLength)
            //@@@@@@@@@
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
            //@@@@@@@@@@
            else
            {
                s_regionLength =(int)highBoundLength;
                t_regionLength = (int)highBoundLength;
            }

            sIndex = Math.floor(sSize/s_regionLength);
            tIndex = Math.floor(tSize/t_regionLength);
            numUsereducers =(int)(sIndex*tIndex);
            numLeftReducers = (int)(numReduers-sIndex*tIndex);
            s_leftLength = (int)(sSize-sIndex * s_regionLength);
            t_leftLength = (int)(tSize-tIndex * t_regionLength);

            //@@@@@@@@@
            //assign reducers id to every region
            for (int i=0;i<Math.floor(sSize/s_regionLength);i++)
            {
                Range s_perfectRangeTemp =new Range((i*s_regionLength),((i+1)*s_regionLength));
                for (int j=0;j<Math.floor(tSize/t_regionLength);j++)
                {
                    Range t_perfectRangeTemp = new Range((j*t_regionLength),((j+1)*t_regionLength));
                    ReducerRegion reducer =new ReducerRegion(s_perfectRangeTemp,t_perfectRangeTemp,id);
                    Reducers.add(reducer);
                    id++;
                }
            }

            //Partition left area
            // find the optimal cut which partitions have minimum difference
            if (s_leftLength!=0 && t_leftLength!=0)
            {
                // two cases. only one reducer left for L area and more than one reducers left.
                if (numLeftReducers ==1)
                {
                  //only one reducer left for L area
                  // pick the partition which the difference between partitions are bigger
                    Reducers.clear();
                    id =0;
                    Range sRangeOneReducerLeft;
                    Range tRangeOneReducerLeft;
                    Range sLeft;
                    Range tLeft;
                    if (Math.min(Math.abs(s_leftLength * tSize - t_leftLength * (sSize - s_leftLength)), Math.abs(t_leftLength * sSize - s_leftLength * (tSize - t_leftLength)))==Math.abs(s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)))
                    {
                        //cut it horizontally and extend right
                        // check which side is longer and make it T. Otherwise make it S
                        if (sSize-s_leftLength>tSize)
                        {
                            // it need to flip
                            sRangeOneReducerLeft = new Range(tRange.low,tRange.high);
                            tRangeOneReducerLeft = new Range(sRange.low,sRange.high-s_leftLength);
                            Partition(tSize-t_leftLength,sSize,numUsereducers,sRangeOneReducerLeft,tRangeOneReducerLeft,!flag);


                        }
                        else
                        {
                            sRangeOneReducerLeft = new Range(sRange.low,sRange.high-s_leftLength);
                            tRangeOneReducerLeft = new Range(tRange.low,tRange.high);
                            Partition(sSize-s_leftLength,tSize,numUsereducers,sRangeOneReducerLeft,tRangeOneReducerLeft,flag);
                        }
                        // assign left row to reducer
                        sLeft = new Range(sRange.high-s_leftLength,sRange.high);
                        tLeft = new Range(tRange.low,tRange.high);
                        ReducerRegion reducer =new ReducerRegion(sLeft,tLeft,id);
                        Reducers.add(reducer);
                        id++;


                    }
                    else
                    // cut it vertically and extend below
                    {
                        // check which side is longer and make it T. Otherwise make it S
                        if (sSize>tSize-t_leftLength)
                        {
                            // it need to flip
                            sRangeOneReducerLeft = new Range(tRange.low,tRange.high-t_leftLength);
                            tRangeOneReducerLeft = new Range(sRange.low,sRange.high);
                            Partition(tSize-t_leftLength,sSize,numUsereducers,sRangeOneReducerLeft,tRangeOneReducerLeft,!flag);

                        }
                        else
                        {
                            sRangeOneReducerLeft = new Range(sRange.low,sRange.high);
                            tRangeOneReducerLeft = new Range(tRange.low,tRange.high-t_leftLength);
                            Partition(sSize-s_leftLength,sSize,numUsereducers,sRangeOneReducerLeft,tRangeOneReducerLeft,flag);
                        }
                        // assign left column to reducer
                        sLeft = new Range(sRange.low,sRange.high);
                        tLeft = new Range(tRange.high-t_leftLength,tRange.high);
                        ReducerRegion reducer =new ReducerRegion(sLeft,tLeft,id);
                        Reducers.add(reducer);
                        id++;
                    }



                }
                else if (numLeftReducers ==0)
                {
                    Reducers.clear();
                    id =0;
                    if (t_leftLength!=0)
                     // one colume left
                    {
                        for (int i=0;i<Math.floor(sSize/s_regionLength);i++)
                            // assign reducers Id to every region and extend the last few regions
                        {
                            Range s_perfectRangeTemp =new Range((i*s_regionLength),((i+1)*s_regionLength));
                            for (int j=0;j<Math.floor(tSize/t_regionLength)-1;j++)
                            {
                                Range t_perfectRangeTemp = new Range((j*t_regionLength),((j+1)*t_regionLength));
                                ReducerRegion reducer =new ReducerRegion(s_perfectRangeTemp,t_perfectRangeTemp,id);
                                Reducers.add(reducer);
                                id++;
                            }
                            // extend t_regionLength
                            Range t_perfectRangeTemp = new Range(tRange.high-(t_leftLength+t_regionLength),tRange.high);
                            ReducerRegion reducer =new ReducerRegion(s_perfectRangeTemp,t_perfectRangeTemp,id);
                            Reducers.add(reducer);
                            id++;

                        }

                    }
                    else
                    // one row left
                    {
                        for (int i=0;i<Math.floor(tSize/t_regionLength);i++)
                        // assign reducers Id to every region and extend the last few regions
                        {
                            Range t_perfectRangeTemp =new Range((i*t_regionLength),((i+1)*t_regionLength));
                            for (int j=0;j<Math.floor(sSize/s_regionLength)-1;j++)
                            {
                                Range s_perfectRangeTemp = new Range((j*s_regionLength),((j+1)*s_regionLength));
                                ReducerRegion reducer =new ReducerRegion(s_perfectRangeTemp,t_perfectRangeTemp,id);
                                Reducers.add(reducer);
                                id++;
                            }
                            // extend s_regionLength
                            Range s_perfectRangeTemp = new Range(sRange.high-(s_leftLength+s_regionLength),sRange.high);
                            ReducerRegion reducer =new ReducerRegion(s_perfectRangeTemp,t_perfectRangeTemp,id);
                            Reducers.add(reducer);
                            id++;

                        }

                    }
                }
                else
                {
                    if (Math.min(Math.abs(s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)),Math.abs(t_leftLength*sSize-s_leftLength*(tSize-t_leftLength)))==Math.abs(s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)))
                    {
                        //cut horizontally
                        if (s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)>0)

                        {
                            //assign more reducers to bigger part.
                            if (s_leftLength<tSize)
                            {
                                sRangeTemp = new Range(sRange.high-s_leftLength,sRange.high);
                                tRangeTemp  = new Range(tRange.low, tRange.high);
                                Partition(s_leftLength, t_leftLength, Math.ceil(numLeftReducers / 2), sRangeTemp, tRangeTemp, flag);

                            }
                            else
                            {
                                System.out.print("s_leftLength>tSize");
                                System.exit(7);
                            }
                            //assign fewer reducers to smaller part.
                            if (t_leftLength<tSize-s_leftLength)
                            {
                                sRangeTemp = new Range(tRange.high-t_leftLength,tRange.high);
                                tRangeTemp  = new Range(tRange.low, tRange.high-s_leftLength);
                                Partition(s_leftLength, t_leftLength, numLeftReducers - Math.ceil(numLeftReducers / 2), sRangeTemp, tRangeTemp, flag);

                            }
                            else
                            {
                                System.out.print("t_leftLength>tSize-s_leftLength");
                                System.exit(7);
                            }

                        }
                        else
                        {
                            //assign fewer reducers to small part.
                            if (s_leftLength<tSize)
                            {
                                sRangeTemp = new Range(sRange.high-s_leftLength,sRange.high);
                                tRangeTemp  = new Range(tRange.low, tRange.high);
                                Partition(s_leftLength, t_leftLength, numLeftReducers - Math.ceil(numLeftReducers / 2), sRangeTemp, tRangeTemp, flag);

                            }
                            else
                            {
                                System.out.print("s_leftLength>tSize");
                                System.exit(7);
                            }
                            //assign more reducers to big part.
                            if (t_leftLength<tSize-s_leftLength)
                            {
                                sRangeTemp = new Range(tRange.high-t_leftLength,tRange.high);
                                tRangeTemp  = new Range(tRange.low, tRange.high-s_leftLength);
                                Partition(s_leftLength, t_leftLength, -Math.ceil(numLeftReducers / 2), sRangeTemp, tRangeTemp, flag);

                            }
                            else
                            {
                                System.out.print("t_leftLength>tSize-s_leftLength");
                                System.exit(7);
                            }
                        }
                    }

                    else// cut vertically 18 and 5
                    {
                        if (t_leftLength*sSize-s_leftLength*(tSize-t_leftLength)>0) //18
                        {
                            //assign more reducers to bigger part.
                            if (t_leftLength<sSize)
                            {
                                sRangeTemp = new Range(tRange.high-t_leftLength,tRange.high);
                                tRangeTemp = new Range(sRange.low,sRange.high);
                                Partition(t_leftLength, sSize, Math.ceil(numLeftReducers / 2), sRangeTemp, tRangeTemp, !flag);
                            }
                            else
                            {
                                //sRangeTemp = new Range(sRange.low,sRange.high-s_leftLength);
                                //tRangeTemp = new Range(tRange.high-t_leftLength,tRange.high);
                                //Test(tSize-t_leftLength,s_leftLength,Math.ceil(numLeftReducers/2),sRangeTemp,tRangeTemp,flag);
                                System.out.print("t_leftLength>sSize");
                                System.exit(7);
                            }
                            //assign fewer reducers to smaller part.
                            if (tSize-t_leftLength>s_leftLength)
                            {
                                sRangeTemp = new Range(sRange.high-s_leftLength,sRange.high);
                                tRangeTemp = new Range(tRange.low,tRange.high-t_leftLength);
                                Partition(s_leftLength, tSize - t_leftLength, numLeftReducers - Math.ceil(numLeftReducers / 2), sRangeTemp, tRangeTemp, flag);
                            }
                            else
                            {
                                System.out.print("tSize-t_leftLength<s_leftLength");
                                System.exit(8);
                            }


                        }
                        //@@@@@@@@@
                        else //5
                        {
                            //assign fewer reducers to smaller part. 5
                            if (t_leftLength<sSize)
                            {
                                sRangeTemp = new Range(tRange.high-t_leftLength,tRange.high);
                                tRangeTemp = new Range(sRange.low,sRange.high);
                                Partition(t_leftLength, sSize, numLeftReducers - Math.ceil(numLeftReducers / 2), sRangeTemp, tRangeTemp, !flag);
                            }
                            else
                            {
                                //sRangeTemp = new Range(sRange.low,sRange.high-s_leftLength);
                                //tRangeTemp = new Range(tRange.high-t_leftLength,tRange.high);
                                //Test(tSize-t_leftLength,s_leftLength,numLeftReducers-Math.ceil(numLeftReducers/2),sRangeTemp,tRangeTemp,flag);
                                System.out.print("t_leftLength>sSize");
                                System.exit(7);
                            }
                            //assign more reducers to bigger part. 18
                            if (tSize-t_leftLength>s_leftLength)
                            {
                                sRangeTemp = new Range(sRange.high-s_leftLength,sRange.high);
                                tRangeTemp = new Range(tRange.low,tRange.high-t_leftLength);
                                Partition(s_leftLength, tSize - t_leftLength, Math.ceil(numLeftReducers / 2), sRangeTemp, tRangeTemp, flag);
                            }
                            else
                            {
                                System.out.print("tSize-t_leftLength<s_leftLength");
                                System.exit(8);
                            }

                        }
                        /////////////////////////////////////////////////////////////////////////////////
                    }
                }
            }
            else if (t_leftLength==0&&s_leftLength!=0)
            {
                // only one row  left
                Range ss = new Range(sRange.high-s_leftLength,sRange.high);
                Range tt = new Range(tRange.low,tRange.high);
                Partition(s_leftLength, tSize, numLeftReducers, ss, tt, flag);



            }
            else if(s_leftLength==0&&t_leftLength!=0)
            {
                //only one column left
                Range ss = new Range(tRange.high-t_leftLength,tRange.high);
                Range tt = new Range(sRange.low,sRange.high);
                Partition(t_leftLength, sSize, numLeftReducers, ss, tt, !flag);
            }

            else //no area left
            {
                return;
            }





        }
    }

//    public void GenerateHashMap(double sSize, double tSize)
//    {
//        String reducersID=" ";
//        for (int i=0;i<sSize;i++)
//        {
//            String line ="S";
//            line = line+Integer.toString(i);
//            for (int j=0;j<numReducer;j++)
//            {
//                if ( i>=Reducers.get(j).S_range.low && i<Reducers.get(j).S_range.high)
//                {
//                    reducersID =reducersID+Integer.toString(Reducers.get(j).reducersID)+" ";
//
//                }
//            }
//            map.put(line, reducersID);
//            reducersID="";
//        }
//
//        for (int i=0;i<tSize;i++)
//        {
//            String line ="T";
//            line = line+Integer.toString(i);
//            for (int j=0;j<numReducer;j++)
//            {
//                if ( i>=Reducers.get(j).T_range.low && i<Reducers.get(j).T_range.high)
//                {
//                    reducersID =reducersID+Integer.toString(Reducers.get(j).reducersID)+" ";
//
//                }
//            }
//            map.put(line, reducersID);
//            reducersID="";
//        }
//        System.out.print("gggg");
//
//    }

    public static void main(String[] args) throws Exception
    {

        //int numReduers = Integer.parseInt(args[0]);
        //int sSize = Integer.parseInt(args[1]);
        //int tSize = Integer.parseInt(args[2]);
        double sSize =5;
        double tSize =10;
        int numReduers = 5;
        Range sRange = new Range(0,(int)sSize);
        Range tRange = new Range(0,(int)tSize);
        boolean flag =true;


        ReducerManager test =new ReducerManager(numReduers);
        test.Partition(sSize, tSize, numReduers, sRange, tRange, flag);
        //stayup.GenerateHashMap(sSize,tSize);
        System.out.print("asd");
    }


}
