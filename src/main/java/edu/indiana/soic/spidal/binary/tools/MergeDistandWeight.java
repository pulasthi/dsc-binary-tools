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

public class MergeDistandWeight {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

    public static void main(String[] args) {
        String inputFile = args[0];
        String inputFilew = args[1];
        String outFile = args[2];

        int numPoints = Integer.valueOf(args[3]);
        endianness = args[4].equals("big") ? ByteOrder.BIG_ENDIAN :
                ByteOrder.LITTLE_ENDIAN;

        generaeSingle(inputFile, inputFilew, outFile, numPoints, endianness, args[5]);
    }

    private static void generaeSingle(String inputFile, String inputFilew, String outFile, int numPoints, ByteOrder endianness, String dataType) {
        try {
            FileChannel fcdata =
                    (FileChannel) Files.newByteChannel(Paths.get(inputFile), StandardOpenOption.READ);
            FileChannel fcweight =
                    (FileChannel) Files.newByteChannel(Paths.get(inputFilew), StandardOpenOption.READ);
            ByteBuffer byteBufferdata = ByteBuffer.allocate((int) fcdata.size());
            ByteBuffer byteBufferweight =
                    ByteBuffer.allocate((int) fcweight.size());

            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBufferdata.order(ByteOrder.BIG_ENDIAN);
                byteBufferweight.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
                byteBufferweight.order(ByteOrder.LITTLE_ENDIAN);
            }
            fcdata.read(byteBufferdata);
            fcweight.read(byteBufferweight);
            byteBufferdata.flip();
            byteBufferweight.flip();

            Buffer bufferdata = null;
            Buffer bufferweight = null;

            short[] shortArrayweight;
            short[] shortArraydata = new short[0];
            switch (dataType) {
                case "short":
                    bufferdata = byteBufferdata.asShortBuffer();
                    shortArraydata = new short[(int) fcdata.size() / 2];
                    ((ShortBuffer) bufferdata).get(shortArraydata);

                    bufferweight = byteBufferweight.asShortBuffer();
                    shortArrayweight = new short[(int) fcweight.size() / 2];
                    ((ShortBuffer) bufferweight).get(shortArrayweight);

                    for (int i = 0; i < shortArraydata.length; i++) {
                        if (shortArrayweight[i] == 0) shortArraydata[i] = 0;
                    }
            }


            ByteBuffer outbyteBufferdata =
                    ByteBuffer.allocate(shortArraydata.length * 2);

            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                outbyteBufferdata.order(ByteOrder.BIG_ENDIAN);
            } else {
                outbyteBufferdata.order(ByteOrder.LITTLE_ENDIAN);
            }

            outbyteBufferdata.clear();
            ShortBuffer shortBuffer =
                    outbyteBufferdata.asShortBuffer();
            shortBuffer.put(shortArraydata);

            FileChannel outIndexfile =
                    new FileOutputStream(outFile).getChannel();
            outIndexfile.write(outbyteBufferdata);
            outIndexfile.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
