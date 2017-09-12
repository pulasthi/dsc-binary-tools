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

        short input[] = new short[37*37];
        int count = 0;
        endianness =  args[2].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        try(BufferedReader br = Files.newBufferedReader(Paths.get(file))){
            String line;
            while ((line = br.readLine()) != null){
                line = line.replace(" ","");
                String splits[] = line.split("\t");
                for (int i = 0; i < splits.length; i++) {
                    String split = splits[i];
                    total += Double.parseDouble(split);
                    short tempval = ((short)(Math.abs(Double.parseDouble(split))*Short.MAX_VALUE));
                    input[count*37 + i] = tempval;
                }
                count++;
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(37*37*2);
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
