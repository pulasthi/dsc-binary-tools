package edu.indiana.soic.spidal.binary.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by pulasthi on 8/5/17.
 */
public class ExtractFromBin {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        String inputFile = args[0];
        String outFile = args[1];
        String newPoints = args[2];
        Set<String> newPointsSet = new HashSet<String>(Arrays.asList(newPoints.split(",")));

        endianness =  args[3].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        switch (args[4]){
            case "short": dataTypeSize = Short.BYTES;
                break;
            case "int": dataTypeSize = Integer.BYTES;
                break;
            case "double": dataTypeSize = Double.BYTES;
                break;
            case "long": dataTypeSize = Long.BYTES;
                break;
            case "float": dataTypeSize = Float.BYTES;
                break;
            case "byte": dataTypeSize = Byte.BYTES;
                break;
            default: dataTypeSize = Short.BYTES;
        }

        extract(inputFile,outFile,endianness,dataTypeSize,args[4],newPointsSet);


    }

    private static void extract(String inputFile, String outFile, ByteOrder endianness, int dataTypeSize, String dataType, Set<String> newPointsSet) {
        try(FileChannel fc = (FileChannel) Files
                .newByteChannel(Paths.get(inputFile), StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate((int)fc.size());
            PrintWriter printWriter = new PrintWriter(new FileWriter(outFile));
            if(endianness.equals(ByteOrder.BIG_ENDIAN)){
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            }else{
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            fc.read(byteBuffer);
            byteBuffer.flip();

            Buffer buffer = null;
            int count = 0;
            int countsmall = 0;
            int countsmallrows = 0;
            switch (dataType){
                case "short":
                    buffer = byteBuffer.asShortBuffer();
                    short[] shortArray = new short[(int)fc.size()/2];
                    ((ShortBuffer)buffer).get(shortArray);
                    int numPoints = (int)Math.sqrt(shortArray.length);
                    for (int i = 0; i < shortArray.length; i++) {
                        int row =  i/numPoints;
                        int col = i%numPoints;
                        if(row  == 0 && col == 1){
                            System.out.println(": " + (double)shortArray[i]/Short.MAX_VALUE);
                        }
                        if(row == 0 && col == 8000){
                            System.out.println(": " + (double)shortArray[i]/Short.MAX_VALUE);
                        }
                        if(row  == 1525 && col == 0){
                            System.out.println(": " + (double)shortArray[i]/Short.MAX_VALUE);
                        }
                        if(row == 7345 && col == 8000){
                            System.out.println(": " + (double)shortArray[i]/Short.MAX_VALUE);
                        }
                        if(row == 7345 && col == 123){
                            System.out.println(": " + (double)shortArray[i]/Short.MAX_VALUE);
                        }

                        short i1 = shortArray[i];
                        double val = (double)i1/Short.MAX_VALUE;
                        if(val == 0){
                            countsmall++;
                            if(col ==0){
                                double sum = 0.0;
                                for (int j = 0; j < numPoints; j++) {
                                    sum += (double)shortArray[i+j]/Short.MAX_VALUE;

                                }
                                if(sum == 0.0 ){
                                    countsmallrows++;
                                    System.out.println("row : " +row);
                                }

                            }
//                                    System.out.println("row : " + row + ":::::::::: col : " + col);
//                                    System.out.println(val);
//                                    System.out.println(i1);
                        }
                        if(newPointsSet.contains(String.valueOf(row))){
                            if(newPointsSet.contains(String.valueOf(col))){
                                if(row==col) continue;
                                printWriter.print(val+",");
                                count++;

//                                if(val == 0){
//                                    countsmall++;
//                                    System.out.println("row : " + row + ":::::::::: col : " + col);
//                                    System.out.println(val);
//                                    System.out.println(i1);
                               // }
                            }
                        }
                    }
                    byteBuffer.clear();
                    byteBuffer = endianness.equals(ByteOrder.BIG_ENDIAN) ? byteBuffer.order(ByteOrder.LITTLE_ENDIAN) :
                            byteBuffer.order(ByteOrder.BIG_ENDIAN);
                    ShortBuffer shortOutputBuffer = byteBuffer.asShortBuffer();
                    
                    shortOutputBuffer.put(shortArray);
                    System.out.println(count);
                    System.out.println(countsmall);
                    System.out.println(countsmallrows);
                    printWriter.close();
                    break;
                case "int":
                    buffer = byteBuffer.asIntBuffer();
                    int[] intArray = new int[(int)fc.size()/4];
                    ((IntBuffer)buffer).get(intArray);
                    byteBuffer.clear();
                    byteBuffer = endianness.equals(ByteOrder.BIG_ENDIAN) ? byteBuffer.order(ByteOrder.LITTLE_ENDIAN) :
                            byteBuffer.order(ByteOrder.BIG_ENDIAN);
                    IntBuffer intOutputBuffer = byteBuffer.asIntBuffer();
                    intOutputBuffer.put(intArray);
                    break;
                case "double":
                    buffer = byteBuffer.asDoubleBuffer();
                    double[] doubleArray = new double[(int)fc.size()/8];
                    ((DoubleBuffer)buffer).get(doubleArray);
                    byteBuffer.clear();
                    byteBuffer = endianness.equals(ByteOrder.BIG_ENDIAN) ? byteBuffer.order(ByteOrder.LITTLE_ENDIAN) :
                            byteBuffer.order(ByteOrder.BIG_ENDIAN);
                    DoubleBuffer doubleOutputBuffer = byteBuffer.asDoubleBuffer();
                    doubleOutputBuffer.put(doubleArray);
                    break;
                case "long":
                    buffer = byteBuffer.asLongBuffer();
                    long[] longArray = new long[(int)fc.size()/8];
                    ((LongBuffer)buffer).get(longArray);
                    byteBuffer.clear();
                    byteBuffer = endianness.equals(ByteOrder.BIG_ENDIAN) ? byteBuffer.order(ByteOrder.LITTLE_ENDIAN) :
                            byteBuffer.order(ByteOrder.BIG_ENDIAN);
                    LongBuffer longOutputBuffer = byteBuffer.asLongBuffer();
                    longOutputBuffer.put(longArray);
                    break;
                case "float":
                    buffer = byteBuffer.asFloatBuffer();
                    float[] floatArray = new float[(int)fc.size()/4];
                    ((FloatBuffer)buffer).get(floatArray);
                    byteBuffer.clear();
                    byteBuffer = endianness.equals(ByteOrder.BIG_ENDIAN) ? byteBuffer.order(ByteOrder.LITTLE_ENDIAN) :
                            byteBuffer.order(ByteOrder.BIG_ENDIAN);
                    FloatBuffer floatOutputBuffer = byteBuffer.asFloatBuffer();
                    floatOutputBuffer.put(floatArray);
                    break;
                case "byte":
                    byteBuffer = endianness.equals(ByteOrder.BIG_ENDIAN) ? byteBuffer.order(ByteOrder.LITTLE_ENDIAN) :
                            byteBuffer.order(ByteOrder.BIG_ENDIAN);
                    break;
            }

    }catch (IOException e){
        e.printStackTrace();
    }
    }
}
