import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Alex on 12/6/14.
 */
public class Test
{
    public static ArrayList<ReducerRegion> Reducers =new ArrayList<ReducerRegion>();
    public static int id =1;


    // flag parameter means if the region flip before. It's initial value is true. When it flip, it turns to opposite
    public static void Partition (double S, double T, double r, Range s_r, Range t_r, boolean fl) throws Exception {
        //Assume |S|<|T|
        //int numReduers = Integer.parseInt(args[0]);
        //int sSize = Integer.parseInt(args[1]);
        //int tSize = Integer.parseInt(args[2]);
        //ArrayList<ReducerRegion> Reducers =new ArrayList<ReducerRegion>();
        double numReduers =r;
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

            //assign reducers id to every region
            for (int i=0;i<Math.floor(sCoefficient);i++)
            {
                Range s_perfectRangeTemp =new Range((i*s_regionLength),((i+1)*s_regionLength));
                for (int j=0;j<Math.floor(tCoefficient);j++)
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
                if (Math.min(Math.abs(s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)),Math.abs(t_leftLength*sSize-s_leftLength*(tSize-t_leftLength)))==Math.abs(s_leftLength*tSize-t_leftLength*(sSize-s_leftLength)))
                {
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
                /////////////////////////////////////////debug successfully//////////////////
                else//18 and 5
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
                            System.out.print("tSize<s_leftLength");
                            System.exit(8);
                        }

                    }
                    /////////////////////////////////////////////////////////////////////////////////
                }
            }
            else if (s_leftLength==0)
            {
                // only one row  left
                Range ss = new Range(tRange.high-t_leftLength,tRange.high);
                Range tt = new Range(sRange.low,sRange.high);
                Partition(tSize - t_leftLength, sSize, numLeftReducers, ss, tt, !flag);

            }
            else
            {
                //only one column left
                Range ss = new Range(sRange.high-s_leftLength,sRange.high);
                Range tt = new Range(tRange.low,tRange.high);
                Partition(tSize - t_leftLength, sSize, numLeftReducers, ss, tt, flag);
            }





        }
    }

    public void GenerateHashMap()
    {
        HashMap<String, String> map = new HashMap<String, String>();


    }
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
        Partition(sSize, tSize, numReduers, sRange, tRange, flag);
        ArrayList<ReducerRegion> partitionResult =Test.Reducers;
        System.out.print("did it");
    }
}
