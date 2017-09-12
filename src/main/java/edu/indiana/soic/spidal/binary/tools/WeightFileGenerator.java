package edu.indiana.soic.spidal.binary.tools;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by pulasthi on 9/8/17.
 */
public class WeightFileGenerator {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

    public static void main(String[] args) {
        String outFile = args[0];
        int numberOfPoints = Integer.parseInt(args[1]);
        double weightValue = Double.parseDouble(args[2]);
        String range = args[3];
        endianness =  args[4].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        String[] rangeValues = range.split("-");
        int start = Integer.parseInt(rangeValues[0]);
        int end = Integer.parseInt(rangeValues[1]);
        short input[] = new short[numberOfPoints];
        ByteBuffer byteBuffer = ByteBuffer.allocate(numberOfPoints*2);
        double defaultValue = 1.0/weightValue;
        double newWeightValue = weightValue/weightValue;
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
                    input[j] = (short)(defaultValue*Short.MAX_VALUE);
                    if(i >= start && j >= start && i <= end && j <= end) input[j] = (short)(newWeightValue*Short.MAX_VALUE);
                }
                ShortBuffer shortOutputBuffer = byteBuffer.asShortBuffer();
                shortOutputBuffer.put(input);
                out.write(byteBuffer);
            }
            out.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
