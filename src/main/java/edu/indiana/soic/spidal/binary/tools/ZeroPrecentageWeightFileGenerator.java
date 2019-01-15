package edu.indiana.soic.spidal.binary.tools;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

/**
 * this class generates weight files for DAMDS algorithm with roughly the given
 * percentage of values set to 0
 */
public class ZeroPrecentageWeightFileGenerator {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

    public static void main(String[] args) {
        String outFile = args[0];
        int numberOfPoints = Integer.parseInt(args[1]);
        double weightValue = Double.parseDouble(args[2]);
        double zeroPrecent = Double.parseDouble(args[3]);
        endianness =  args[4].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        Random random = new Random();
        short input[] = new short[numberOfPoints];
        ByteBuffer byteBuffer = ByteBuffer.allocate(numberOfPoints*2);
        if(endianness.equals(ByteOrder.BIG_ENDIAN)){
            byteBuffer.order(ByteOrder.BIG_ENDIAN);
        }else{
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        }


        try {
            FileChannel out = new FileOutputStream(outFile).getChannel();

            for (int i = 0; i < numberOfPoints; i++) {
                byteBuffer.clear();
                for (int j = 0; j < numberOfPoints; j++) {
                    if(random.nextDouble() > zeroPrecent){
                        input[j] = (short)(weightValue*Short.MAX_VALUE);
                    }else{
                        input[j] = 0;
                    }
                }
                ShortBuffer shortOutputBuffer = byteBuffer.asShortBuffer();
                shortOutputBuffer.put(input);
                out.write(byteBuffer);
            }
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
