package edu.indiana.soic.spidal.binary.tools;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

/**
 * Created by pulasthi on 10/12/17.
 */
public class HistogramGeneratorAL {
    public static void main(String[] args) {
        String inFile = args[0];
        String outFile = args[1];
        String range = args[2];// goes from 0 to numP-1 ex 0-250
        int start = Integer.valueOf(range.split("-")[0]);
        int end = Integer.valueOf(range.split("-")[1]);
        boolean onlySample = Boolean.valueOf(args[3]);
        boolean invert = Boolean.valueOf(args[4]);
        int numSamples = 0;
        String line =  "";
        int row;
        int col;
        int AL;
        int count = 0;
        if(onlySample) numSamples = Integer.valueOf(args[5]);
        //double pointsInRange = (double)numPoints*(end-start+1);
        //double prob = numSamples/pointsInRange;
        double prob = 0.0002;
        Random random = new Random();

        try(BufferedReader br = Files.newBufferedReader(Paths.get(inFile))){
            PrintWriter printWriter = new PrintWriter(new FileWriter(outFile));

            while ((line = br.readLine()) != null){
                if(onlySample && random.nextDouble() < prob) {
                    String splits[] = line.split("\\s+");
                    row = Integer.parseInt(splits[0]);
                    col = Integer.parseInt(splits[1]);
                    AL = Integer.parseInt(splits[2]);
                    if(!invert){
                        if(row < start || col < start) continue;
                        if(row > end || col > end) continue;
                        printWriter.write(AL + ",");
                        count++;
                    }else{
                        if(row > end && col < end && col > start){
                            printWriter.write(AL + ",");
                            count++;
                        }else if(row > start && row < end && col > end){
                            printWriter.write(AL + ",");
                            count++;
                        }else{
                            continue;
                        }
                    }
                    if(count > numSamples) {
                        printWriter.flush();
                        printWriter.close();
                        System.out.println("Count : " + count);
                        return;
                    }
                }else{
                    String splits[] = line.split("\\s+");
                    row = Integer.parseInt(splits[0]);
                    col = Integer.parseInt(splits[1]);
                    AL = Integer.parseInt(splits[2]);
                    if(row < start || col < start) continue;
                    if(row > end || col > end) continue;
                    printWriter.write(AL + ",");
                    count++;
                }
            }

            System.out.println("Count : " + count);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
