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
 * Given a bin file and a K (number of references)
 * Extracts K samples out of each row and creates a new text file
 */
public class DistanceSamplerForDL {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        String inputFile = args[0];
        String outFile = args[1];
        int K = Integer.parseInt(args[2]);
        int numPoitns = Integer.parseInt((args[3]));
        endianness =  args[4].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        int[] indexList = generateIndexList(K, numPoitns);

        extractIndexList(indexList, inputFile, outFile, numPoitns, K);
    }

    private static void extractIndexList(int[] indexList, String inputFile, String outFile, int numPoints, int K) {
        try(FileChannel fc = (FileChannel) Files
                .newByteChannel(Paths.get(inputFile), StandardOpenOption.READ)) {
            FileChannel out = new FileOutputStream(outFile).getChannel();
            ByteBuffer bufferOut =  ByteBuffer.allocate(numPoints*K*2);
            if (endianness.equals(ByteOrder.BIG_ENDIAN)) {
                bufferOut.order(ByteOrder.BIG_ENDIAN);
            } else {
                bufferOut.order(ByteOrder.LITTLE_ENDIAN);
            }
            bufferOut.clear();
            ShortBuffer shortOutputBuffer = bufferOut.asShortBuffer();

            ByteBuffer byteBuffer = ByteBuffer.allocate(numPoints*2);
            if(endianness.equals(ByteOrder.BIG_ENDIAN)){
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            }else{
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }
            short[] shortArray = new short[numPoints];
            short[] shortArrayOut = new short[K];
            for (int i = 0; i < numPoints; i++) {
                fc.read(byteBuffer);
                byteBuffer.flip();
                ShortBuffer buffer = byteBuffer.asShortBuffer();
                buffer.get(shortArray);
                for (int j = 0; j < K; j++) {
                    shortArrayOut[j] = shortArray[indexList[j]];
                }
                shortOutputBuffer.put(shortArrayOut);
                byteBuffer.clear();
            }

            out.write(bufferOut);
            out.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private static int[] generateIndexList(int K, int numPoints){
        int spacing = numPoints/K;
        int[] indexList = new int[K];
        int index = 0;
        for (int i = 0; i < numPoints; i += spacing) {
            indexList[index++] = i;
            if(index == indexList.length)
                break;
        }
        return indexList;
    }
}
