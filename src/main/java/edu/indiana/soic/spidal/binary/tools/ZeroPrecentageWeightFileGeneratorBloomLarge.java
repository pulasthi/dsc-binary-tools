package edu.indiana.soic.spidal.binary.tools;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * this class generates weight files for DAMDS algorithm with roughly the given
 * percentage of values set to 0
 */
public class ZeroPrecentageWeightFileGeneratorBloomLarge {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

    public static void main(String[] args) {
        String outFile = args[0];
        String inFile = args[1];
        int numberOfPoints = Integer.parseInt(args[2]);
        double weightValue = Double.parseDouble(args[3]);
        double zeroPrecent = Double.parseDouble(args[4]);
        endianness = args[5].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        int counts[] = new int[Short.MAX_VALUE];
        Buffer bufferdata = null;
        short maxVal = 0;

        try {
            FileChannel fcdata =
                    (FileChannel) Files.newByteChannel(Paths.get(inFile), StandardOpenOption.READ);
            ByteBuffer byteBufferdata = ByteBuffer.allocate((int) fcdata.size());

            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBufferdata.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
            }

            fcdata.read(byteBufferdata);
            byteBufferdata.flip();
            bufferdata = byteBufferdata.asShortBuffer();
            short[] shortArraydata = new short[(int) fcdata.size() / 2];
            ((ShortBuffer) bufferdata).get(shortArraydata);

            for (short shortArraydatum : shortArraydata) {
                counts[shortArraydatum]++;
            }

            int total = numberOfPoints * numberOfPoints;
            int percent = (int) (total * (1.0 - zeroPrecent));

            int tempcount = 0;
            for (int curr : counts) {
                tempcount += curr;
                maxVal++;
                if (tempcount > percent) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            FileChannel fcdata =
                    (FileChannel) Files.newByteChannel(Paths.get(inFile), StandardOpenOption.READ);
            ByteBuffer byteBufferdata = ByteBuffer.allocate((int) fcdata.size());

            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBufferdata.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
            }
            int zeros = 0;
            fcdata.read(byteBufferdata);
            byteBufferdata.flip();
            bufferdata = byteBufferdata.asShortBuffer();
            short[] shortArraydata = new short[(int) fcdata.size() / 2];
            ((ShortBuffer) bufferdata).get(shortArraydata);


            FileChannel out = new FileOutputStream(outFile).getChannel();
            short input[] = new short[numberOfPoints * numberOfPoints];
            ByteBuffer byteBuffer = ByteBuffer.allocate(numberOfPoints * numberOfPoints * 2);
            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            byteBuffer.clear();
            int countin = 0;
            for (short val : shortArraydata) {
                int i = countin / numberOfPoints;
                int j = countin % numberOfPoints;
                if (i == j) {
                    input[countin++] = 0;
                    zeros++;
                } else if (val > maxVal) {
                    input[countin++] = 0;
                    zeros++;
                } else {
                    input[countin++] = (short) (weightValue * Short.MAX_VALUE);

                }
            }


            for (int i = 0; i < numberOfPoints; i++) {
                double tempsum = 0;
                for (int j = 0; j < numberOfPoints; j++) {
                    tempsum += input[i*numberOfPoints+j];
                }
                if(tempsum == 0){
                    System.out.println("zero row");
                    input[i] = (short) (weightValue * Short.MAX_VALUE);
                    input[i*numberOfPoints] = (short) (weightValue * Short.MAX_VALUE);
                }
            }
            System.out.println("Zeros : " + zeros + " Zero p%% " + (double) zeros / (numberOfPoints * numberOfPoints));
            ShortBuffer shortOutputBuffer = byteBuffer.asShortBuffer();
            shortOutputBuffer.put(input);
            out.write(byteBuffer);
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
