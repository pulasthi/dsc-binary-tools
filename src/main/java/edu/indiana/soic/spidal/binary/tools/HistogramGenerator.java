package edu.indiana.soic.spidal.binary.tools;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;

/**
 * Extracts values from bin files and creates histograms.
 * currently only extracts values so they can be plotted in R
 * Data is considered to be a distance matrix of NxN
 */
public class HistogramGenerator {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        String binFile = args[0];
        String outFile = args[1];
        int numPoints = Integer.valueOf(args[2]);
        String range = args[3];// goes from 0 to numP-1 ex 0-250
        int start = Integer.valueOf(range.split("-")[0]);
        int end = Integer.valueOf(range.split("-")[1]);
        endianness =  args[4].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        switch (args[5]){
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

        boolean onlySample = Boolean.valueOf(args[6]);
        boolean isRangeCol = Boolean.valueOf(args[7]);
        int numSamples = 0;
        if(onlySample) numSamples = Integer.valueOf(args[7]);
        long startPos = ((long)start)*numPoints*dataTypeSize;
        ByteBuffer byteBuffer = ByteBuffer.allocate(numPoints*dataTypeSize);
        if(endianness.equals(ByteOrder.BIG_ENDIAN)){
            byteBuffer.order(ByteOrder.BIG_ENDIAN);
        }else{
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        Random random = new Random();

        try(RandomAccessFile rmaf = new RandomAccessFile(Paths.get(binFile).toString(), "r")) {
            PrintWriter printWriter = new PrintWriter(new FileWriter(outFile));
            rmaf.seek(startPos);
            byte[] temp = new byte[numPoints*dataTypeSize];
            Buffer buffer = null;
            if(!onlySample){
                //Extract all points and write to text file as double values.
                int count = 0;

                for (int i = 0; i <= (end - start); i++) {
                    byteBuffer.clear();
                    rmaf.read(temp);
                    byteBuffer.put(temp);
                    byteBuffer.flip();
                    buffer = byteBuffer.asShortBuffer();
                    short[] shortArray = new short[numPoints];
                    ((ShortBuffer)buffer).get(shortArray);
                    for (int j = 0; j < shortArray.length; j++) {
                        if(j < start || j > end) continue;
                        short i1 = shortArray[j];
                        printWriter.write(String.format("%.4f", (double)i1/Short.MAX_VALUE) + ",");
                        count++;
                        if(count%10000 == 0) System.out.println(count);
                    }
                }
            }else{
                double pointsInRange = (double)numPoints*(end-start+1);
                double prob = numSamples*100/pointsInRange;
                System.out.printf("Prob :" + prob ) ;
                int count = 0;
                for (int i = 0; i <= (end - start); i++) {
                    byteBuffer.clear();
                    rmaf.read(temp);
                    byteBuffer.put(temp);
                    byteBuffer.flip();
                    buffer = byteBuffer.asShortBuffer();
                    short[] shortArray = new short[numPoints];
                    ((ShortBuffer)buffer).get(shortArray);
                    for (int j = 0; j < shortArray.length; j++) {
                        if(j < start || j > end) continue;
                        short i1 = shortArray[j];
                        if(random.nextDouble() < prob){
                            printWriter.write(String.format("%.4f", (double)i1/Short.MAX_VALUE) + ",");
                            count++;
                            if(count > numSamples) break;
                            if(count%10000 == 0) System.out.println(count);
                        }
                    }
                }
                System.out.println(count);
            }
            printWriter.flush();
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
