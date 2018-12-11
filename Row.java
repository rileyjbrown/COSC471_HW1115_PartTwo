package com.company;

import java.util.Random;

public class Row {

    // Our record size comes from an int, a char array of 1000, and a long.
    // Their byte sizes added together create the record size of 2012.
    public static final int recordSize = 2012;

    // Initialize the length of the second attribute (the 1000 char string)
    public final static int SecondAttributeLength = 1000;

    // Initialize your three attributes
    private int firstAttribute;
    private char[] secondAttribute;
    private long ID;

    // Create the Row to be used in other classes
    public Row(int firstAttribute, char[] secondAttribute, long ID){
        this.firstAttribute = firstAttribute;
        this.ID = ID;

        // Inititialize Second Attribute Length
        this.secondAttribute = new char[SecondAttributeLength];
        for(int i = 0; i < secondAttribute.length; i++){
            this.secondAttribute[i] = secondAttribute[i];
        }
    }

    // Create a getter for the first attribute (an int)
    public int getFirstAttribute() {
        return firstAttribute;
    }

    // Create a getter for the second attribute (a 1000 char string)
    public char[] getSecondAttribute() {
        return secondAttribute;
    }

    // Create a getter for the third attribute (a unique 64 bit long) which we have called ID
    public long getID() {
        return ID;
    }
}
