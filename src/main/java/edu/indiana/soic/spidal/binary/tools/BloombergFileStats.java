package edu.indiana.soic.spidal.binary.tools;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class BloombergFileStats {
    public static void main(String[] args) {
        String fileName = args[0];


        try {
            BufferedReader bf = new BufferedReader(new FileReader(fileName));
            String line = null;
            String splits[];
            int currntMax = Integer.MIN_VALUE;
            long count = 0;
            double sum = 0;
            while ((line = bf.readLine()) != null){
                splits = line.split("\\s+");
                int row = Integer.valueOf(splits[0]);
                int col = Integer.valueOf(splits[1]);
                sum += Double.valueOf(splits[2]);
                count++;
                if(currntMax > row){
                    throw new IllegalStateException("File not in order" + count);
                }
                currntMax = row;
                if(count%10000000 == 0){
                    System.out.print(".");
                }

            }

            System.out.println("Total lines in file :" + count);
            System.out.println("Total SUM in file :" + sum);
            System.out.println("Average in file :" + sum/count);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
