package edu.indiana.soic.spidal.binary.tools.mpitools;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class CountZeros {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

    public static void main(String[] args) {

        String inputFile = args[0];
        endianness = args[1].equals("big") ? ByteOrder.BIG_ENDIAN :
                ByteOrder.LITTLE_ENDIAN;

        try {
            FileChannel fcdata =
                    (FileChannel) Files.newByteChannel(Paths.get(inputFile), StandardOpenOption.READ);

            ByteBuffer byteBufferdata = ByteBuffer.allocate((int) fcdata.size());
            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBufferdata.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
            }

            fcdata.read(byteBufferdata);
            byteBufferdata.flip();

            Buffer buffer = null;
            buffer = byteBufferdata.asShortBuffer();
            short[] shortArray = new short[(int) fcdata.size() / 2];
            ((ShortBuffer) buffer).get(shortArray);
            int count = 0;
            for (int i = 0; i < shortArray.length; i++) {
                if (shortArray[i] >= 0) count++;
            }

            System.out.println("Count Zeros " + count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
