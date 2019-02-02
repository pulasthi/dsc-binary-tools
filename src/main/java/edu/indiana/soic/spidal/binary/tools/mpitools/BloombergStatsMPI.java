package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPIException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BloombergStatsMPI {
    public static void main(String[] args) {
        try {
            System.out.println("Got Here");
            ParallelOps.setupParallelism(args);
            Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
            String fileDir = args[0];

            int totalSplits = 192;
            String filePrefirx = "part_";
            int filesPerProc = totalSplits / ParallelOps.worldProcsCount;

            double max = Double.MIN_VALUE;
            double min = Double.MAX_VALUE;
            double count = 0;
            double missingCount = 0;
            int currentMax = 0;
            int countWrong = 0;

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
                    if (splits.length < 3) {
                        countWrong++;
                        continue;
                    }
                    if (splits[0].equals("") || splits[1].equals("") || splits[2].equals("")) {
                        countWrong++;
                        continue;
                    }
                    int row = Integer.valueOf(splits[0]);
                    int col = Integer.valueOf(splits[1]);
                    double value = Double.valueOf(splits[2]);
                    max = (value > max) ? value : max;
                    min = (min > value) ? value : min;

                    count++;
                    if(ParallelOps.worldProcRank == 0 && (count % 20000000 == 0)){
                        System.out.print(".");
                    }
                    if (currentMax > row) {
                        throw new IllegalStateException("File not in order" + count);
                    }

                    if (row > currentMax) {
                        missingCount += (row - currentMax - 1);
                    }
                    currentMax = row;
                }


            }

            max = ParallelOps.allReduceMax(max);
            min = ParallelOps.allReduceMin(min);
            count = ParallelOps.allReduce(count);
            missingCount = ParallelOps.allReduce(missingCount);
            countWrong = ParallelOps.allReduce(countWrong);


            if (ParallelOps.worldProcRank == 0) {
                System.out.printf("Max : %.5f\nMin : %.5f\nTotalLinks : %.2f\nMissingCount : %.2f\nCountWrong : %d",
                        max, min, count, missingCount, countWrong);
            }
        } catch (MPIException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
