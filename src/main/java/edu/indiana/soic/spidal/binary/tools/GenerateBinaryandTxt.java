package edu.indiana.soic.spidal.binary.tools;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by pulasthi on 11/2/16.
 */
public class GenerateBinaryandTxt {
    private static ByteOrder endianness = ByteOrder.LITTLE_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        try {
            int N = Integer.valueOf(args[0]);
            int D = Integer.valueOf(args[1]);
            String outputfile = args[2];
            String outputfiletxt = args[3];
            double input[] = new double[N * D];
            endianness = args[3].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

            PrintWriter outWriter = new PrintWriter(new FileWriter(outputfiletxt));


            for (int i = 0; i < N; i++) {
                for (int j = 0; j < D; j++) {
                    double v = Math.random();
                    input[i*j + j] = v;
                    if(j < D - 1){
                        outWriter.print(v+",\t");
                    }else{
                        outWriter.print(v);
                    }
                }
                outWriter.println();
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(N * D * 8);
            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }

            byteBuffer.clear();
            DoubleBuffer doubleOutputBuffer = byteBuffer.asDoubleBuffer();
            doubleOutputBuffer.put(input);
            FileChannel out = new FileOutputStream(outputfile).getChannel();
            out.write(byteBuffer);
            out.close();
            outWriter.flush();
            outWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
