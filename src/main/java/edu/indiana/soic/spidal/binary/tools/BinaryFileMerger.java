package edu.indiana.soic.spidal.binary.tools;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Created by pulasthi on 8/8/17.
 */
public class BinaryFileMerger {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

    public static void main(String[] args) {
        String inputFolder = args[0];
        String fileNametemplate = args[1];
        int numFiles = Integer.parseInt(args[2]);
        String outFile = args[3];
        endianness =  args[4].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

        try{
            FileChannel fc;
            Buffer buffer;
            FileChannel out = new FileOutputStream(outFile).getChannel();

            for (int i = 0; i < numFiles; i++) {
                String fileName = String.format(fileNametemplate,i);
                Path path = Paths.get(inputFolder,fileName);
                fc = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ);
                ByteBuffer byteBuffer = ByteBuffer.allocate((int)fc.size());
                if(endianness == ByteOrder.BIG_ENDIAN ){
                    byteBuffer.order(ByteOrder.BIG_ENDIAN);
                }else{
                    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                }
                fc.read(byteBuffer);
                byteBuffer.flip();
                out.write(byteBuffer);
            }
            out.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
