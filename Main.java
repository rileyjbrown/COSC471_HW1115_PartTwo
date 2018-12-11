package com.company;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        try{
            // Create a filepath to save our data to
            File file = new File("results.csv");

            // If a file exists already, delete it so we can make the new file
            if(file.exists()){
                file.delete();
            }

            // Create the .csv file
            file.createNewFile();

            // Write the file to the created file name
            FileWriter fileWriter = new FileWriter("results.csv");

            // Create the headers for the table for when we open it in Excel
            fileWriter.write("Table Size,Hit Count No Hash, Miss Count No Hash, Hit Count Hash, Miss Count Hash");
            fileWriter.write(System.lineSeparator());
            fileWriter.close();
        }
        catch(IOException ex){

        }

        // Perform operations using our other classes to go up to 10,000
        for(int i = 1; i <= 50; i++){
            // Perform the Join operation
            JoinOperation operation = new JoinOperation(i);

            // Grab the results of the joining of tables R1 and S1
            JoinResult noHash = operation.joinR1OnS1();

            // Grab the results of the joining of tables R1 and S2
            JoinResult withHash = operation.joinR1OnS2();

            // Print out the results of the joins
            // Currently commented out because we are writing all the information to an excel file instead
            // Uncomment this section if you want to see the information output in the console

            System.out.println("********************************************");
            System.out.println("Table Size: " + i);
            System.out.println("");
            System.out.println("R1 joined with S1: ");
            System.out.println("Hit Count: " + noHash.getHitCount());
            System.out.println("Miss Count: " + noHash.getMissCount());
            System.out.println("");
            System.out.println("R1 joined with S2: ");
            System.out.println("Hit Count: " + withHash.getHitCount());
            System.out.println("Miss Count: " + withHash.getMissCount());
            System.out.println("********************************************");

            // Write the results in the excel file under the headers created above
            try{
                FileWriter fileWriter = new FileWriter("results.csv", true);
                fileWriter.write(i + "," + noHash.getHitCount() + "," + noHash.getMissCount() + "," +
                        withHash.getHitCount() + "," + withHash.getMissCount());
                fileWriter.write(System.lineSeparator());
                fileWriter.close();
            }
            catch(IOException ex){

            }
        }
        Scanner keyboard = new Scanner(System.in);
        keyboard.next();
    }
}
