package edu.indiana.soic.spidal.binary.tools;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by pulasthi on 12/7/17.
 */
public class BinaryToTxt {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        // arg[3] takes one of the primitive type names in lower case
        String file = args[0];
        String outputfile = args[1];
        endianness =  args[2].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        switch (args[3]){
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

        ConverttoTxt(file,outputfile,endianness,dataTypeSize,args[3]);


    }

    private static void ConverttoTxt(String filename, String outputfilename, ByteOrder endianness, int dataTypeSize, String dataType) {
        try(FileChannel fc = (FileChannel) Files
                .newByteChannel(Paths.get(filename), StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate((int)fc.size());

            if(endianness.equals(ByteOrder.BIG_ENDIAN)){
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            }else{
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            fc.read(byteBuffer);
            byteBuffer.flip();
            PrintWriter outWriter = new PrintWriter(new FileWriter(outputfilename));

            Buffer buffer = null;
            double[] doubleArray = new double[(int)fc.size()/2];
            int numPoints = (int)Math.sqrt((int)fc.size()/2);

            switch (dataType){
                case "short":
                    buffer = byteBuffer.asShortBuffer();
                    short[] shortArray = new short[(int)fc.size()/2];
                    ((ShortBuffer)buffer).get(shortArray);
                    for (int i = 0; i < shortArray.length; i++) {
                        short i1 = shortArray[i];
                        doubleArray[i] = (double)shortArray[i]/Short.MAX_VALUE;
                    }
                    for (int i = 0; i < numPoints; i++) {
                        for (int j = 0; j < numPoints; j++) {
                            outWriter.printf("%.6f",doubleArray[i*numPoints + j]);
                            if(j < numPoints - 1){
                                outWriter.print(",");
                            }
                        }
                        outWriter.println();
                    }
                    break;
                case "int":
                    buffer = byteBuffer.asIntBuffer();
                    int[] intArray = new int[(int)fc.size()/4];
                    ((IntBuffer)buffer).get(intArray);

                    break;
                case "double":
                    buffer = byteBuffer.asDoubleBuffer();
                    ((DoubleBuffer)buffer).get(doubleArray);
                    byteBuffer.clear();
                    break;
                case "long":
                    buffer = byteBuffer.asLongBuffer();
                    long[] longArray = new long[(int)fc.size()/8];
                    ((LongBuffer)buffer).get(longArray);
                    byteBuffer.clear();
                    break;
                case "float":
                    buffer = byteBuffer.asFloatBuffer();
                    float[] floatArray = new float[(int)fc.size()/4];
                    ((FloatBuffer)buffer).get(floatArray);
                    byteBuffer.clear();
                    break;
                case "byte":
                    byteBuffer = endianness.equals(ByteOrder.BIG_ENDIAN) ? byteBuffer.order(ByteOrder.LITTLE_ENDIAN) :
                            byteBuffer.order(ByteOrder.BIG_ENDIAN);
                    break;
            }

           outWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
