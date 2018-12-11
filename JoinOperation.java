package com.company;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

public class JoinOperation {

    // Create a variable for tablesize
    private int tableSize;

    // Create file names so we can make the tables Random Access Files
    public static final String tableR1FileName = "tableR1";
    public static final String tableS1FileName = "tableS1";
    public static final String tableS2FileName = "tableS2";

    // Perform the join operation by inserting data into tables
    public JoinOperation(int tableSize){
        this.tableSize = tableSize;

        // Create file names for all three tables
        File tableR1File= new File("tableR1");
        File tableS1File= new File("tableS1");
        File tableS2File= new File("tableS2");

        // If the file for R1 alreadys exists, delete it
        if(tableR1File.exists()){
            tableR1File.delete();
        }

        // If the file for S1 alreadys exists, delete it
        if(tableS1File.exists()){
            tableS1File.delete();
        }

        // If the file for S2 alreadys exists, delete it
        if(tableS2File.exists()){
            tableS2File.delete();
        }

        // Create random access files of all three tables.
        try(RandomAccessFile tableR1 = new RandomAccessFile(tableR1FileName, "rw");
            RandomAccessFile tableS1 = new RandomAccessFile(tableS1FileName, "rw");
            RandomAccessFile tableS2 = new RandomAccessFile(tableS2FileName, "rw"))
        {
            // Fill all three tables with the fillTable method
            fillTable(tableR1, tableSize);
            fillTable(tableS1, tableSize);
            fillTable(tableS2, tableSize * 2);
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }

        // Insert into tableR1 using the insertIntoTable method created
        for(long i = 0; i < tableSize; i++){
            insertIntoTableR1(i);
        }

        // Insert into tableS1 using the insertIntoTable method created
        for(long i = 0; i < tableSize; i++){ ;
            insertIntoTableS1(i);
        }

        // Insert into tableS2 using the insertIntoTable method created
        for(long i = 0; i < tableSize; i++){
            insertIntoTableS2(i);
        }
    }

    // Create a method to insert data into table R1
    public void insertIntoTableR1(long insertLocation){

        // Initialize the ID
        long ID = insertLocation + 1;

        Row row = new Row(Utilities.createRandomInt(), Utilities.createRandomCharArray(Row.SecondAttributeLength), ID);

        // Write data into tableR1
        try(RandomAccessFile tableR1 = new RandomAccessFile(tableR1FileName, "rw")){
            // Seek to the correct position
            tableR1.seek(insertLocation*Row.recordSize);

            // Insert attribute one
            tableR1.writeInt(row.getFirstAttribute());

            // Insert attribute two
            for(int i = 0; i < Row.SecondAttributeLength; i++){
                tableR1.writeChar(row.getSecondAttribute()[i]);
            }

            // Insert attribute three
            tableR1.writeLong(row.getID());

        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }

    // Create a method to insert data into table S1
    public void insertIntoTableS1(long insertLocation){

        // Initialize the ID
        long ID = insertLocation + 1;

        Row row = new Row(Utilities.createRandomInt(), Utilities.createRandomCharArray(Row.SecondAttributeLength), ID);

        // Write our attributes to tableS1
        try(RandomAccessFile tableS1 = new RandomAccessFile(tableS1FileName, "rw")){
            // Seek to the correct position
            tableS1.seek(insertLocation*Row.recordSize);

            // Insert attribute one
            tableS1.writeInt(row.getFirstAttribute());

            // Insert attribute two
            for(int i = 0; i < Row.SecondAttributeLength; i++){
                tableS1.writeChar(row.getSecondAttribute()[i]);
            }

            // Insert attribute three
            tableS1.writeLong(row.getID());


        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }

    public void insertIntoTableS2(long insertLocation){

        // Initialize the ID
        long ID = insertLocation + 1;

        // Set the index
        int index = new Integer((int)ID).hashCode()%tableSize;


        try(RandomAccessFile tableS2 = new RandomAccessFile(tableS2FileName, "rw")){
            tableS2.seek(index*Row.recordSize);

            // Initialize the row to contain our attributes
            Row row = new Row(Utilities.createRandomInt(), Utilities.createRandomCharArray(Row.SecondAttributeLength), ID);

            // Read the selected row from the table
            Row rowToCheck = readRowFromTable(tableS2);

            if(rowToCheck.getID() == 0){
                // Seek to the correct position
                tableS2.seek(index*Row.recordSize);

                // Insert attribute one into tableS2
                tableS2.writeInt(row.getFirstAttribute());

                // Insert attribute two into tableS2
                for(int i = 0; i < Row.SecondAttributeLength; i++){
                    tableS2.writeChar(row.getSecondAttribute()[i]);
                }

                // Insert attribute three into tableS2
                tableS2.writeLong(row.getID());

            }
            else {
                try (RandomAccessFile overflowTable = new RandomAccessFile("overflowTable_" + ID, "rw")) {

                    // Seek to the correct position in the overflow table
                    overflowTable.seek(overflowTable.length());

                    // Insert attribute one into the overflow table
                    overflowTable.writeInt(row.getFirstAttribute());

                    // Insert attribute two into the overflow table
                    for (int i = 0; i < Row.SecondAttributeLength; i++) {
                        overflowTable.writeChar(row.getSecondAttribute()[i]);
                    }

                    // Insert attribute three into the overflow table
                    overflowTable.writeLong(row.getID());

                } catch (IOException ex) {
                    System.out.println(ex.getMessage());
                }

            }
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }

    public JoinResult joinR1OnS1(){
        // Intialize hitCount and missCount to 0 to start
        int hitCount = 0;
        int missCount = 0;

        // Set up tableR1 and tableS1 so we can perform the join
        try(RandomAccessFile tableR1 = new RandomAccessFile(tableR1FileName, "rw");
            RandomAccessFile tableS1 = new RandomAccessFile(tableS1FileName, "rw")){

            for(int i = 0; i < tableR1.length()/Row.recordSize; i++){
                // Seek to the correct position
                tableR1.seek(i*Row.recordSize);

                // Read the correct row from tableR1
                Row row = readRowFromTable(tableR1);

                for(int j = 0; j < tableS1.length()/Row.recordSize; j++){
                    // Seek to the correct position in tableS2
                    tableS1.seek(j*Row.recordSize);

                    // Read the row from tableS2
                    Row row2 = readRowFromTable(tableS1);

                    // If there is data at the index for tables S1 and S2, there is a hit
                    if(row.getID() == row2.getID()){
                        hitCount++;
                    }

                    // If there is not data at the index of the tables, there is a miss
                    else{
                        missCount++;
                    }
                }
            }
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
        // Return the hitCount and missCount resulting from the join
        return new JoinResult(hitCount, missCount);
    }

    public JoinResult joinR1OnS2(){

        // Initialize the hitCount and missCount
        int hitCount = 0;
        int missCount = 0;

        try(RandomAccessFile tableR1 = new RandomAccessFile(tableR1FileName, "rw");
            RandomAccessFile tableS2 = new RandomAccessFile(tableS2FileName, "rw")){

            for(int i = 0; i < tableR1.length()/Row.recordSize; i++){
                // Seek tableR1
                tableR1.seek(i*Row.recordSize);

                // Read from tableR1
                Row row = readRowFromTable(tableR1);

                // Set the index
                int index = new Integer((int)row.getID()).hashCode()%tableSize;

                // Seek tableS2
                tableS2.seek(index*Row.recordSize);

                // Read from tableS2
                Row row2 = readRowFromTable(tableS2);

                // If there is information in both tables at the index, there is a hit
                if(row.getID() == row2.getID()){
                    hitCount++;
                }

                // If there is not data at the index of both tables, there is a miss
                else {
                    missCount++;

                    try (RandomAccessFile overflowTable = new RandomAccessFile("overflowTable_" + row.getID(), "rw")) {
                        for(int j = 0; j < overflowTable.length()/Row.recordSize; j++){
                            // Seek in the overflow table
                            overflowTable.seek(j*Row.recordSize);

                            // Create row 3 from the overflow table
                            Row row3 = readRowFromTable(overflowTable);

                            // If row3 from the overflow table is equal to the row from tableR1, there is a hit
                            if(row3.getID() == row.getID()){
                                hitCount++;
                            }
                            // Else there is a miss
                            else{
                                missCount++;
                            }
                        }


                    } catch (IOException ex) {
                        System.out.println(ex.getMessage());
                    }
                }
            }
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }

        // Return the hitCount and missCount of the join
        return new JoinResult(hitCount, missCount);
    }

    private void fillTable(RandomAccessFile table, int numOfRows){

        try{
            for(int i = 0; i < numOfRows; i++){
                // Write in attribute one
                table.writeInt(0);

                // Write in attribute two
                for(int j = 0; j < Row.SecondAttributeLength; j++){
                    table.writeChar(0);
                }

                // Write in attribute three
                table.writeLong(0);
            }
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }

    public Row readRowFromTable(RandomAccessFile table){

        // Initialize all three attributes for a record
        int firstAttrib = 0;
        char[] secondAttrib = new char[Row.SecondAttributeLength];
        long thirdAttrib = 0;


        try{
            // Read the first attribute
            firstAttrib = table.readInt();

            // Read the second attribute
            for(int i = 0; i < Row.SecondAttributeLength; i++){
                secondAttrib[i] = table.readChar();
            }

            // Read the third attribute
            thirdAttrib = table.readLong();
        }
        catch(IOException ex) {
            System.out.println(ex.getMessage());
        }

        // Create a row to return with all three attributes
        Row row = new Row(firstAttrib, secondAttrib, thirdAttrib);
        return row;
    }
}
