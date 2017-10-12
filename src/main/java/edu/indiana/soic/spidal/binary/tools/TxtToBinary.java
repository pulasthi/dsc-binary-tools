package edu.indiana.soic.spidal.binary.tools;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by pulasthi on 9/13/16.
 */
public class TxtToBinary {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;
    private static int dataTypeSize = Short.BYTES;

    public static void main(String[] args) {
        String file = args[0];
        String outputfile = args[1];
        double total = 0.0;
        int numPoints = Integer.parseInt(args[2]);

        short input[] = new short[numPoints*numPoints];
        int count = 0;
        endianness =  args[3].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        try(BufferedReader br = Files.newBufferedReader(Paths.get(file))){
            String line;
            while ((line = br.readLine()) != null){
                String splits[] = line.split("\\s+");
                for (int i = 0; i < splits.length; i++) {
                    String split = splits[i];
                    double tempd = Double.parseDouble(split)/10; // special case remove later
                    total += tempd;
                    short tempval = ((short)(Math.abs(tempd)*Short.MAX_VALUE));
                    input[count*numPoints + i] = tempval;
                }
                count++;
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(numPoints*numPoints*2);
            if(endianness.equals(ByteOrder.BIG_ENDIAN)){
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            }else{
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }

            byteBuffer.clear();
            ShortBuffer shortOutputBuffer = byteBuffer.asShortBuffer();
            shortOutputBuffer.put(input);
            FileChannel out = new FileOutputStream(outputfile).getChannel();
            out.write(byteBuffer);
            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
