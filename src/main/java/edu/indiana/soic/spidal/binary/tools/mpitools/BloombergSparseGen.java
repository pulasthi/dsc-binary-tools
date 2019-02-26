package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPIException;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BloombergSparseGen {

    //Min and max of all the files, this was calculated using the stats calculation
    static double min = 0.61600;
    static double max = 14950.00;
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

    public static void main(String[] args) {
        try {
            ParallelOps.setupParallelism(args);

            Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
            String inFileDir = args[0];
            String outfileDir = args[1];
            endianness = args[2].equals("big") ? ByteOrder.BIG_ENDIAN :
                    ByteOrder.LITTLE_ENDIAN;

            int totalSplits = 192;
            String filePrefirx = "part_";
            int filesPerProc = totalSplits / ParallelOps.worldProcsCount;

            String outFileIndex = outfileDir + "SparseIn_" + ParallelOps.worldProcRank;
            String outFiledata = outfileDir + "SparseDa_" + ParallelOps.worldProcRank;
            FileChannel outIndexfile =
                    new FileOutputStream(outFileIndex).getChannel();
            FileChannel outDatafile =
                    new FileOutputStream(outFiledata).getChannel();


            for (int i = 0; i < filesPerProc; i++) {
                int currentrow = -1;
                List<Item> rowList = new ArrayList();
                int fileIndex = ParallelOps.worldProcRank * filesPerProc + i;
                String fileId = (fileIndex < 100) ? "0" : "";
                fileId += (fileIndex < 10) ? "0" + fileIndex : "" + fileIndex;
                String filePath = inFileDir + filePrefirx + fileId;
                BufferedReader bf = new BufferedReader(new FileReader(filePath));
                String line = null;
                String splits[];

                //if this is not the first file we need to check if the previous file will read the first
                //few rows so we can skip them
                int pFileRow = -1;
                if(fileIndex > 0){
                    pFileRow = checkPreviousRow(fileIndex - 1, inFileDir, filePrefirx);
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
                            printRowToFile(currentrow, rowList, outIndexfile, outDatafile);
                            if(ParallelOps.worldProcRank == 0){
                                if(row < 1){
                                    for (Item item : rowList) {
                                        System.out.println("" + row + "\t" + item.col + "\t" + item.value );
                                    }
                                }
                            }
                            rowList.clear();
                            currentrow = row;
                            rowList.add(new Item(col, dist));
                        }else{
                            currentrow = row;
                            rowList.add(new Item(col, dist));
                        }
                    }else{
                        rowList.add(new Item(col, dist));
                    }
                }

                //If the file ended we need to handle the last row
                if(fileIndex < totalSplits - 1){
                    checkNextFile(rowList, currentrow, inFileDir, fileIndex + 1, filePrefirx);
                }
                Collections.sort(rowList);
                printRowToFile(currentrow, rowList, outIndexfile, outDatafile);

            }

            outIndexfile.close();
            outDatafile.close();
        } catch (MPIException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printRowToFile(int row, List<Item> rowList, FileChannel outIndexfile, FileChannel outDatafile) throws IOException {
        short[] outputdata = new short[rowList.size()];
        int[] outputindex = new int[rowList.size()*2];

        int count = 0;
        for (Item item : rowList) {
            outputindex[count*2] = row;
            outputindex[count*2 + 1] = item.col;
            if(item.value > 1.0 || item.value < 0.0) throw new IllegalStateException("Distance out of range : " + item.value );

            outputdata[count] = (short)(item.value*Short.MAX_VALUE);
        }

        ByteBuffer outbyteBufferdata =
                ByteBuffer.allocate(outputdata.length * 2);
        ByteBuffer outbyteBufferindex =
                ByteBuffer.allocate(outputindex.length * 4);
        if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
            outbyteBufferdata.order(ByteOrder.BIG_ENDIAN);
            outbyteBufferindex.order(ByteOrder.BIG_ENDIAN);
        } else {
            outbyteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
            outbyteBufferindex.order(ByteOrder.LITTLE_ENDIAN);
        }
        outbyteBufferdata.clear();
        outbyteBufferindex.clear();

        ShortBuffer shortOutputBuffer =
                outbyteBufferdata.asShortBuffer();
        shortOutputBuffer.put(outputdata);

        IntBuffer intOutputBuffer = outbyteBufferindex.asIntBuffer();
        intOutputBuffer.put(outputindex);

        outIndexfile.write(outbyteBufferindex);
        outDatafile.write(outbyteBufferdata);
    }

    private static int checkPreviousRow(int index, String fileDir, String filePrefirx) throws IOException {
        String fileId = (index < 100) ? "0" : "";
        fileId += (index < 10) ? "0" + index : "" + index;
        String filePath = fileDir + filePrefirx + fileId;
        BufferedReader bf = new BufferedReader(new FileReader(filePath));
        String line = bf.readLine();
        String splits[] = line.split("\\s+");;
        return Integer.valueOf(splits[0]) - 1;
    }

    private static void checkNextFile(List<Item> rowList, int currentrow, String fileDir, int index, String filePrefirx) throws IOException {
        String fileId = (index < 100) ? "0" : "";
        fileId += (index < 10) ? "0" + index : "" + index;
        String filePath = fileDir + filePrefirx + fileId;
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
