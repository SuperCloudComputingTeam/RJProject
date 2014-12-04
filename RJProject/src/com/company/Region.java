package com.company;

import java.util.ArrayList;

/**
 * Created by syq3b on 12/3/2014.
 */
public class Region {
    //Each region will have a range, origin, and reducers id
    Range range;
    String origin;
    ArrayList<Integer> reducersID;

    public Region(){
        range = new Range();
        origin =  "";
        reducersID = new ArrayList<Integer>();
    }

    public Region(Range r, String s, ArrayList<Integer> list){
        range = r;
        origin = s;
        reducersID = list;
    }
}
