package edu.indiana.soic.spidal.binary.tools.mpitools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * calculate partition points
 */
public class BloombergPartCalc {

    private static ByteOrder endianness = ByteOrder.LITTLE_ENDIAN;
    private static int numPoints = 14905940;

    public static void main(String[] args) {
        Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
        String indicesPath = args[0];
        int[] counts = new int[numPoints];
        long count = 0;
        long entryCount = 0;
        long blockSize = 1024 * 1024 * 1000; // 200Mb, the index file will take 200*4

        try {

            FileChannel fcIndex = (FileChannel) Files
                    .newByteChannel(Paths.get(indicesPath),
                            StandardOpenOption.READ);
            long totalLength = fcIndex.size();
            long rbSizeIn = (blockSize > totalLength) ?
                    totalLength : blockSize;
            // for each data value which is a short |2| value

            long currentRead = 0;
            ByteBuffer outbyteBufferindex =
                    ByteBuffer.allocate((int) rbSizeIn);
            outbyteBufferindex.order(endianness);

            while (currentRead < totalLength) {
                outbyteBufferindex.clear();

                rbSizeIn = (blockSize > (totalLength - currentRead)) ?
                        (totalLength - currentRead) : blockSize;

                //if the size is smaller create two new smaller buffs
                if (rbSizeIn != outbyteBufferindex.capacity()) {
                    System.out.println("#### Using new ByteBuffer");
                    outbyteBufferindex = ByteBuffer.allocate((int) rbSizeIn);
                    outbyteBufferindex.order(endianness);
                    outbyteBufferindex.clear();
                }
                fcIndex.read(outbyteBufferindex, currentRead * 2);
                outbyteBufferindex.flip();


                while (outbyteBufferindex.hasRemaining()) {
                    count++;
                    int row = outbyteBufferindex.getInt();
                    int col = outbyteBufferindex.getInt();
                    counts[row]++;
                    entryCount++;
                    if (row != col) {
                        entryCount++;
                        counts[col]++;
                    }
                    if(count % 50000000 == 0){
                        System.out.print(".");
                    }
                }

                currentRead += rbSizeIn;
            }
            int[] rows = new int[225];
            long perProc = count / 224;
            int index = 0;
            for (int i = 1; i < rows.length; i++) {
                long temp = 0;
                while (temp < perProc) {
                    temp += counts[index++];
                }
                rows[i] = index;
            }
            rows[224] = numPoints;

            System.out.println("Stats : total count : " + count + " en Count : " + entryCount);
            System.out.println("splits : " + Arrays.toString(rows));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
