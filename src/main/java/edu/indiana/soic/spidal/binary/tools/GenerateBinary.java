package edu.indiana.soic.spidal.binary.tools;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by pulasthi on 11/2/16.
 */
public class GenerateBinary {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        int N = Integer.valueOf(args[0]);
        int D = Integer.valueOf(args[1]);
        String outputfile = args[2];
        double input[] = new double[N*D];
        endianness =  args[3].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        for (int i = 0; i < N*D; i++) {
            input[i] = Math.random();
        }

        try{
            ByteBuffer byteBuffer = ByteBuffer.allocate(N*D*8);
            if(endianness.equals(ByteOrder.BIG_ENDIAN)){
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            }else{
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }

            byteBuffer.clear();
            DoubleBuffer doubleOutputBuffer = byteBuffer.asDoubleBuffer();
            doubleOutputBuffer.put(input);
            FileChannel out = new FileOutputStream(outputfile).getChannel();
            out.write(byteBuffer);
            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
