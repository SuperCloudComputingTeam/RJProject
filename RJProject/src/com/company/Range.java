package com.company;

import java.util.ArrayList;

/**
 * Created by syq3b on 12/3/2014.
 */
public class Range {
    public int range;
    public ArrayList<Integer> reducers=null;


    public Range(int range){
        this.range=range;
        reducers= new ArrayList<Integer>();
    }
}
