package edu.indiana.soic.spidal.binary.tools;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class SparseBinFileCreator {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;
    private static int chunkSize = 400000;

    public static void main(String[] args) {
        String inputFile = args[0];
        String inputFilew = args[1];
        String outFileIndex = args[2];
        String outFileData = args[3];
        //the result would be a NxN matrix where N is extractPoints
        int numPoints = Integer.valueOf(args[4]);
        endianness = args[5].equals("big") ? ByteOrder.BIG_ENDIAN :
                ByteOrder.LITTLE_ENDIAN;

        extract(inputFile, inputFilew, outFileIndex, outFileData, numPoints, endianness,
                args[6]);
    }

    private static void extract(String inputFile, String inputFilew,
                                String outFileIndex, String outFiledata,
                                int numPoints,
                                ByteOrder endianness, String dataType) {
        try {
            FileChannel fcdata =
                    (FileChannel) Files.newByteChannel(Paths.get(inputFile), StandardOpenOption.READ);
            FileChannel fcweight =
                    (FileChannel) Files.newByteChannel(Paths.get(inputFilew), StandardOpenOption.READ);
            ByteBuffer byteBufferdata = ByteBuffer.allocate((int) fcdata.size());
            ByteBuffer byteBufferweight =
                    ByteBuffer.allocate((int) fcweight.size());

            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBufferdata.order(ByteOrder.BIG_ENDIAN);
                byteBufferweight.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
                byteBufferweight.order(ByteOrder.LITTLE_ENDIAN);
            }
            fcdata.read(byteBufferdata);
            fcweight.read(byteBufferweight);
            byteBufferdata.flip();
            byteBufferweight.flip();

            FileChannel outIndexfile =
                    new FileOutputStream(outFileIndex).getChannel();

            FileChannel outDatafile =
                    new FileOutputStream(outFiledata).getChannel();

            Buffer bufferdata = null;
            Buffer bufferweight = null;

            List<Short> outData = new ArrayList();
            List<Integer> outIndex = new ArrayList();
            long size = fcdata.size() / 2;
            long currentCount = 0;
            int indexCount = 0;
            bufferdata = byteBufferdata.asShortBuffer();
            bufferweight = byteBufferweight.asShortBuffer();
            int zeroCount = 0;
            while (currentCount < size) {
                outData = new ArrayList();
                outIndex = new ArrayList();
                int currentChunk = (size - currentCount) < chunkSize ? (int) (size - currentCount) : chunkSize;

                switch (dataType) {
                    case "short":
                        short[] shortArraydata = new short[currentChunk];
                        ((ShortBuffer) bufferdata).get(shortArraydata);

                        short[] shortArrayweight =
                                new short[currentChunk];
                        System.out.println("CC " + currentChunk + " shorAWLen " + shortArrayweight.length + "Buffer length " +  ((ShortBuffer) bufferweight).capacity());
                        ((ShortBuffer) bufferweight).get(shortArrayweight);
                        for (int i = 0; i < shortArraydata.length; i++) {
                            int row = (i + indexCount) / numPoints;
                            int col = (i + indexCount) % numPoints;
                            if (shortArrayweight[i] > 0) {
                                outData.add(shortArraydata[i]);
                                outIndex.add(row);
                                outIndex.add(col);
                            }else{
                                zeroCount++;
                            }
                        }
                        indexCount += shortArraydata.length;

                        short[] outputdata = new short[outData.size()];
                        for (int i = 0; i < outputdata.length; i++) {
                            outputdata[i] = outData.get(i);
                        }

                        int[] outputindex = new int[outIndex.size()];
                        for (int i = 0; i < outputindex.length; i++) {
                            outputindex[i] = outIndex.get(i);
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
                currentCount += currentChunk;
                bufferdata.position((int) currentCount);
                bufferweight.position((int) currentCount);
                System.out.println(" Completed " + currentCount + "/" + size);
            }

            outIndexfile.close();
            outDatafile.close();
            System.out.printf("ZeroCount " + zeroCount);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
