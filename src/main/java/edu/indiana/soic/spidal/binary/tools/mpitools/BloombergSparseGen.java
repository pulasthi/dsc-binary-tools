package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPIException;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BloombergSparseGen {

    //Min and max of all the files, this was calculated using the stats calculation
    static double min = 1;
    static double max = 2;

    public static void main(String[] args) {
        try {
            ParallelOps.setupParallelism(args);

            Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
            String fileDir = args[0];

            int totalSplits = 48;
            String filePrefirx = "part_";
            int filesPerProc = totalSplits / ParallelOps.worldProcsCount;


            for (int i = 0; i < filesPerProc; i++) {
                int currentrow = -1;
                List<Item> rowList = new ArrayList();
                int fileIndex = ParallelOps.worldProcRank * filesPerProc + i;
                String fileId = (fileIndex < 10) ? "0" + fileIndex : "" + fileIndex;
                String filePath = fileDir + fileId;
                BufferedReader bf = new BufferedReader(new FileReader(filePath));
                String line = null;
                String splits[];

                //if this is not the first file we need to check if the previous file will read the first
                //few rows so we can skip them
                int pFileRow = -1;
                if(fileIndex > 0){
                    pFileRow = checkPreviousRow(fileIndex - 1, fileDir);
                }
                while ((line = bf.readLine()) != null) {
                    splits = line.split("\\s+");
                    //Reduce 1 because the data starts from 1 not 0 as we want
                    int row = Integer.valueOf(splits[0]) - 1;
                    if(row == pFileRow) continue;

                    int col = Integer.valueOf(splits[1]) - 1;
                    double score = Double.valueOf(splits[2]);
                    double dist = (1 / score - 1 / max) * min * max / (max - min);

                    if (currentrow != row) {
                        if (currentrow != -1) {
                            Collections.sort(rowList);
                            //printRowToFile(rowList);
                            if(ParallelOps.worldProcRank == 0){
                                if(row < 1){
                                    for (Item item : rowList) {
                                        System.out.println("" + row + "\t" + item.col + "\t" + item.value );
                                    }
                                }
                            }
                            rowList.clear();
                        }
                    }


                    rowList.add(new Item(col, dist));
                    Collections.sort(rowList);
                    //printRowToFile(rowList);

                }

                //If the file ended we need to handle the last row
                if(fileIndex < totalSplits - 1){
                    checkNextFile(rowList, currentrow, fileDir, fileIndex + 1);

                }


            }


        } catch (MPIException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int checkPreviousRow(int index, String fileDir) throws IOException {
        String fileId = (index < 10) ? "0" + index : "" + index;
        String filePath = fileDir + fileId;
        BufferedReader bf = new BufferedReader(new FileReader(filePath));
        String line = bf.readLine();
        String splits[] = line.split("\\s+");;
        return Integer.valueOf(splits[0]) - 1;
    }

    private static void checkNextFile(List<Item> rowList, int currentrow, String fileDir, int index) throws IOException {
        String fileId = (index < 10) ? "0" + index : "" + index;
        String filePath = fileDir + fileId;
        BufferedReader bf = new BufferedReader(new FileReader(filePath));
        String line = null;
        String splits[];
        while ((line = bf.readLine()) != null) {
            splits = line.split("\\s+");
            //Reduce 1 because the data starts from 1 not 0 as we want
            int row = Integer.valueOf(splits[0]) - 1;
            int col = Integer.valueOf(splits[1]) - 1;
            double score = Double.valueOf(splits[2]);
            double dist = (1 / score - 1 / max) * min * max / (max - min);

            if(row == currentrow){
                rowList.add(new Item(col, dist));
            }else{
                break;
            }
        }
    }

    public static class Item implements Comparable<Item> {
        int col;
        double value;

        public Item(int col, double value) {
            this.col = col;
            this.value = value;
        }


        @Override
        public int compareTo(@NotNull Item item) {
            return this.col - item.col;
        }
    }
}
