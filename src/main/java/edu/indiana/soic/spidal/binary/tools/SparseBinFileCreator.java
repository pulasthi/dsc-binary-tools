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

    public static void main(String[] args) {
        String inputFile = args[0];
        String inputFilew = args[1];
        String outFileIndex = args[2];
        String outFileData = args[3];
        //the result would be a NxN matrix where N is extractPoints
        int numPoints = Integer.valueOf(args[4]);
        endianness = args[5].equals("big") ? ByteOrder.BIG_ENDIAN :
                ByteOrder.LITTLE_ENDIAN;

        switch (args[4]) {
            case "short":
                dataTypeSize = Short.BYTES;
                break;
            case "int":
                dataTypeSize = Integer.BYTES;
                break;
            case "double":
                dataTypeSize = Double.BYTES;
                break;
            case "long":
                dataTypeSize = Long.BYTES;
                break;
            case "float":
                dataTypeSize = Float.BYTES;
                break;
            case "byte":
                dataTypeSize = Byte.BYTES;
                break;
            default:
                dataTypeSize = Short.BYTES;
        }
        extract(inputFile, inputFilew, outFileIndex, outFileData, numPoints, endianness,
                args[5]);
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
            ByteBuffer byteBufferdata = ByteBuffer.allocate((int)fcdata.size());
            ByteBuffer byteBufferweight =
                    ByteBuffer.allocate((int)fcweight.size());

            if(endianness.equals(ByteOrder.BIG_ENDIAN)){
                byteBufferdata.order(ByteOrder.BIG_ENDIAN);
                byteBufferweight.order(ByteOrder.BIG_ENDIAN);
            }else{
                byteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
                byteBufferweight.order(ByteOrder.LITTLE_ENDIAN);
            }
            fcdata.read(byteBufferdata);
            fcweight.read(byteBufferweight);
            byteBufferdata.flip();
            byteBufferweight.flip();

            Buffer bufferdata = null;
            Buffer bufferweight = null;

            List<Double> outData = new ArrayList();
            List<Integer> outIndex = new ArrayList();

            switch (dataType) {
                case "short":
                    bufferdata = byteBufferdata.asShortBuffer();
                    short[] shortArraydata = new short[(int)fcdata.size()/2];
                    ((ShortBuffer)bufferdata).get(shortArraydata);

                    bufferweight = byteBufferweight.asShortBuffer();
                    short[] shortArrayweight =
                            new short[(int)fcweight.size()/2];
                    ((ShortBuffer)bufferweight).get(shortArrayweight);

                    for (int i = 0; i < shortArraydata.length; i++) {
                        int row = i / numPoints;
                        int col = i % numPoints;
                        if(shortArrayweight[i] > 0){
                            outData.add((double)shortArraydata[i]/Short.MAX_VALUE);
                            outIndex.add(row);
                            outIndex.add(col);
                        }
                    }
            }

            double[] outputdata = new double[outData.size()];
            for (int i = 0; i < outputdata.length; i++) {
                outputdata[i] = outData.get(i);
            }

            int[] outputindex = new int[outIndex.size()];
            for (int i = 0; i < outputindex.length; i++) {
                outputindex[i] = outIndex.get(i);
            }

            ByteBuffer outbyteBufferdata =
                    ByteBuffer.allocate(outputdata.length * 8);
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

            DoubleBuffer doubleOutputBuffer =
                    outbyteBufferdata.asDoubleBuffer();
            doubleOutputBuffer.put(outputdata);

            IntBuffer intOutputBuffer = outbyteBufferindex.asIntBuffer();
            intOutputBuffer.put(outputindex);


            FileChannel outIndexfile =
                    new FileOutputStream(outFileIndex).getChannel();
            outIndexfile.write(outbyteBufferindex);
            outIndexfile.close();



            FileChannel outDatafile =
                    new FileOutputStream(outFiledata).getChannel();
            outDatafile.write(outbyteBufferdata);
            outDatafile.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
