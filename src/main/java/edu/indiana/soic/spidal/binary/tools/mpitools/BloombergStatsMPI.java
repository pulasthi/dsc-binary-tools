package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPI;
import mpi.MPIException;

import java.io.*;

public class BloombergStatsMPI {
    public static void main(String[] args) {
        try {
            System.out.println("Got Here");
            ParallelOps.setupParallelism(args);
            Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
            String fileDir = args[0];
            String outFile = args[1];
            String outFile2 = args[1];

            int totalSplits = 192;
            String filePrefirx = "part_";
            int filesPerProc = totalSplits / ParallelOps.worldProcsCount;

            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double sum = 0.0;
            double count = 0;
            double missingCount = 0;
            int currentMax = -1;
            int countWrong = 0;
            int countOver1k = 0;
            int count100to1k = 0;
            int countover10 = 0;
            double[] histogram = new double[1013];
            double[] histogram60 = new double[60];

            double sdsum = 0.0;

            for (int i = 0; i < filesPerProc; i++) {
                int fileIndex = ParallelOps.worldProcRank * filesPerProc + i;
                String fileId = (fileIndex < 100) ? "0" : "";
                fileId += (fileIndex < 10) ? "0" + fileIndex : "" + fileIndex;
                String filePath = fileDir + filePrefirx + fileId;
                BufferedReader bf = new BufferedReader(new FileReader(filePath));
                String line = null;
                String splits[];
                while ((line = bf.readLine()) != null) {
                    splits = line.split("\\s+");

                    int row = Integer.valueOf(splits[0]);
                    int col = Integer.valueOf(splits[1]);
                    double value = Double.valueOf(splits[2]);
                    sum += value;
                    max = (value > max) ? value : max;
                    min = (min > value) ? value : min;


                    //Histo 60 graph
                    int index60 = (int)Math.floor(value/250);
                    histogram60[index60]++;


                    //Histo my
                    if (value > 1000) {
                        countOver1k++;
                        histogram[1012]++;
                    } else if (value > 100) {
                        count100to1k++;
                        histogram[1011]++;
                    } else if (value >= 10) {
                        countover10++;
                        histogram[1010]++;
                    }else if(value < 10 && value >= 1){
                        int ind = (int)(Math.floor(value));
                        histogram[1000+ind]++;
                    }else{
                        int ind = (int)(Math.floor(value*1000));
                        histogram[ind]++;
                    }

                    count++;
                    if (ParallelOps.worldProcRank == 0 && (count % 20000000 == 0)) {
                        System.out.print(".");
                    }
                    if (currentMax > row) {
                        throw new IllegalStateException("File : " + fileId + " not in order at line : " + count);
                    }

                    if (currentMax > 0 && row > currentMax) {
                        missingCount += (row - currentMax - 1);
                    }
                    currentMax = row;
                }


            }
            max = ParallelOps.allReduceMax(max);
            min = ParallelOps.allReduceMin(min);
            count = ParallelOps.allReduce(count);
            missingCount = ParallelOps.allReduce(missingCount);
            countOver1k = ParallelOps.allReduce(countOver1k);
            countover10 = ParallelOps.allReduce(countover10);
            count100to1k = ParallelOps.allReduce(count100to1k);
            ParallelOps.allReduce(histogram, MPI.SUM, ParallelOps.worldProcsComm);
            sum = ParallelOps.allReduce(sum);
            countWrong = ParallelOps.allReduce(countWrong);
            double mean = sum / count;


            if(ParallelOps.worldProcRank==0){
                PrintWriter outWriterhistMds = new PrintWriter(new FileWriter(outFile));
                for (double val : histogram) {
                    outWriterhistMds.print(val+",");
                }
                outWriterhistMds.flush();
                outWriterhistMds.close();


                //60
                PrintWriter outWriterhist60 = new PrintWriter(new FileWriter(outFile2));
                for (double val : histogram60) {
                    outWriterhist60.print(val+",");
                }

                outWriterhist60.flush();
                outWriterhist60.close();
            }

//            for (int i = 0; i < filesPerProc; i++) {
//                int fileIndex = ParallelOps.worldProcRank * filesPerProc + i;
//                String fileId = (fileIndex < 100) ? "0" : "";
//                fileId += (fileIndex < 10) ? "0" + fileIndex : "" + fileIndex;
//                String filePath = fileDir + filePrefirx + fileId;
//                BufferedReader bf = new BufferedReader(new FileReader(filePath));
//                String line = null;
//                String splits[];
//                while ((line = bf.readLine()) != null) {
//                    splits = line.split("\\s+");
//                    double value = Double.valueOf(splits[2]);
//                    sdsum += (value - mean) * (value - mean);
//                }
//
//
//            }


            sdsum = ParallelOps.allReduce(sdsum);
            double sd = Math.sqrt(sdsum / count - 1);
            if (ParallelOps.worldProcRank == 0) {
                System.out.printf("Max : %.5f\nMin : %.5f\nTotalLinks : %.2f\nMissingCount : %.2f\nCountWrong : %d" +
                                "\nMean : %.5f\n Over1k %d\n 100to1k %d\n 10to100 %d \nSD %.5f\n",
                        max, min, count, missingCount, countWrong, mean, countOver1k, count100to1k, countover10, sd);
            }
        } catch (MPIException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
