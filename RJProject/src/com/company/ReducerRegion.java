package com.company;
import java.util.ArrayList;

/**
 * Created by Alex on 12/7/14.
 */
public class ReducerRegion {
    //Each ReducerRegion will only have a S_range, a T_range and it's own reducer id
    Range S_range;
    Range T_range;
    int reducersID;

    public ReducerRegion(){
        S_range = new Range();
        T_range = new Range();
        reducersID = 0;
    }

    public ReducerRegion(Range s, Range t, int id){
        S_range = s;
        T_range = t;
        reducersID = id;
    }
}
