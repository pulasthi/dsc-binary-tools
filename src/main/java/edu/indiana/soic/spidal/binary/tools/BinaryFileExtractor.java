package edu.indiana.soic.spidal.binary.tools;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Given a binary file and a range to extract this class will extract the
 * given submatrix range and create a new bin file.
 */
public class BinaryFileExtractor {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        String inputFile = args[0];
        String outFile = args[1];
        //the result would be a NxN matrix where N is extractPoints
        int numPoints = Integer.valueOf(args[2]);
        int extractPoints = Integer.valueOf(args[3]);
        endianness = args[4].equals("big") ? ByteOrder.BIG_ENDIAN :
                ByteOrder.LITTLE_ENDIAN;

        switch (args[5]) {
            case "short":
                dataTypeSize = Short.BYTES;
                break;
            case "int":
                dataTypeSize = Integer.BYTES;
                break;
            case "double":
                dataTypeSize = Double.BYTES;
                break;
            case "long":
                dataTypeSize = Long.BYTES;
                break;
            case "float":
                dataTypeSize = Float.BYTES;
                break;
            case "byte":
                dataTypeSize = Byte.BYTES;
                break;
            default:
                dataTypeSize = Short.BYTES;
        }
        extract(inputFile, outFile, numPoints, extractPoints, endianness,
                args[5]);
    }

    private static void extract(String inputFile, String outFile, int numPoints,
                                int extractPoints, ByteOrder endianness,
                                String dataType) {
        try (FileChannel fc = (FileChannel) Files
                .newByteChannel(Paths.get(inputFile), StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate((int) fc.size());
            ByteBuffer outbyteBuffer = ByteBuffer.allocate(extractPoints * extractPoints * 2);
            short output[] = new short[extractPoints * extractPoints];

            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                outbyteBuffer.order(ByteOrder.BIG_ENDIAN);
            } else {
                outbyteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            outbyteBuffer.clear();
            ShortBuffer shortOutputBuffer = outbyteBuffer.asShortBuffer();


            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            } else {
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            fc.read(byteBuffer);
            byteBuffer.flip();

            Buffer buffer = null;

            switch (dataType) {
                case "short":
                    buffer = byteBuffer.asShortBuffer();
                    short[] shortArray = new short[(int) fc.size() / 2];
                    ((ShortBuffer) buffer).get(shortArray);
                    for (int i = 0; i < shortArray.length; i++) {
                        int row = i / numPoints;
                        int col = i % numPoints;
                        if (row >= extractPoints || col >= extractPoints)
                            continue;
                        output[row * extractPoints + col] = shortArray[i];
                    }
                    break;

            }

            shortOutputBuffer.put(output);
            FileChannel out = new FileOutputStream(outFile).getChannel();
            out.write(outbyteBuffer);
            out.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
