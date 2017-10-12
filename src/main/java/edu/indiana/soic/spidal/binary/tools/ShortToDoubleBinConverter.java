package edu.indiana.soic.spidal.binary.tools;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by pulasthi on 10/11/17.
 */
public class ShortToDoubleBinConverter {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        String file = args[0];
        String outputfile = args[1];
        endianness =  args[2].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        try(FileChannel fc = (FileChannel) Files
                .newByteChannel(Paths.get(file), StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) fc.size());

            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            fc.read(byteBuffer);
            byteBuffer.flip();

            Buffer buffer = null;
            buffer = byteBuffer.asShortBuffer();
            short[] shortArray = new short[(int)fc.size()/2];
            ((ShortBuffer)buffer).get(shortArray);
            double[] doubleArray = new double[(int)fc.size()/2];
            for (int i = 0; i < shortArray.length; i++) {
                short i1 = shortArray[i];
                doubleArray[i] = (double)shortArray[i]/Short.MAX_VALUE;
            }

            FileChannel out = new FileOutputStream(outputfile).getChannel();
            ByteBuffer byteBuffer2 = ByteBuffer.allocate(doubleArray.length*8);
            byteBuffer2.clear();
            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBuffer2.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBuffer2.order(ByteOrder.LITTLE_ENDIAN);
            }
            DoubleBuffer doubleBuffer = byteBuffer2.asDoubleBuffer();
            doubleBuffer.put(doubleArray);
            out.write(byteBuffer2);
            out.close();
        }catch (IOException e){
            e.printStackTrace();
        }

    }
}
