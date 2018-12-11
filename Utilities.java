package com.company;

import java.util.Random;

public class Utilities {
    // Create a method toe creat a random char array
    public static char[] createRandomCharArray(int maxLength){
        // Initialize the char array randomCharacters
        char[] randomCharacters;

        // Create a variable random
        Random random = new Random();

        randomCharacters = new char[random.nextInt(maxLength)];

        // Insert random characters into the randomCharacters array
        for(int i = 0; i < randomCharacters.length; i++){

            char insertChar = (char)random.nextInt(127);

            randomCharacters[i] = insertChar;
        }
        // Return the result of the insert
        return randomCharacters;
    }

    // Create a method to create random integers
    public static int createRandomInt(){

        Random random = new Random();

        // Return a random integer
        return random.nextInt(Integer.MAX_VALUE);

    }
}
