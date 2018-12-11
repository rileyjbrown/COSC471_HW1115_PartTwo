package com.company;

public class JoinResult {

    // Create variables for hitCount and missCount
    private int hitCount;
    private int missCount;

    // Create a method for JoinResult to be used in JoinOperation
    public JoinResult(int hitCount, int missCount){
        this.hitCount = hitCount;
        this.missCount = missCount;
    }

    // Create a getter for hitCount
    public int getHitCount() {
        return hitCount;
    }

    // Create a getter for missCount
    public int getMissCount() {
        return missCount;
    }
}
