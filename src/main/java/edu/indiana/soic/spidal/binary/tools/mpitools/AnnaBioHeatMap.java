package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPI;
import mpi.MPIException;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class AnnaBioHeatMap {
    private static ByteOrder endianness = ByteOrder.BIG_ENDIAN;

    public static void main(String[] args) {
        try {
            ParallelOps.setupParallelism(args);
            Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
            int para = ParallelOps.worldProcsCount;
            int numPoints = Integer.valueOf(args[0]);
            String mdspoints = args[1];
            String distanceFile = args[2];
            String outFileDir = args[3];
            double[][] points = new double[numPoints][3];
            Map<Integer, Integer> indexMap = new HashMap<Integer, Integer>();
            int pointsPerProc = numPoints/para;
            ParallelOps.worldProcsComm.barrier();
            double max = 1.0;
            double min = 0.0;
            double minOri = 0.0;
            double maxOri = 1.0;
            double heatmap[] = new double[1000*1000];
            double histtroOri[] = new double[1000];
            double histtroMDS[] = new double[1000];
            try {
                //read points
                BufferedReader bf = new BufferedReader(new FileReader(mdspoints));
                String line;
                String splits[];
                int index = 0;
                while ((line = bf.readLine()) != null){
                    splits = line.split("\\s+");
                    double x = Double.valueOf(splits[1]);
                    double y = Double.valueOf(splits[2]);
                    double z = Double.valueOf(splits[3]);
                    points[index][0] = x;
                    points[index][1] = y;
                    points[index][2] = z;
                    index++;
                }

            }catch (IOException e){
                e.printStackTrace();
            }

            ParallelOps.worldProcsComm.barrier();
            Utils.printMessage("All done reading");
            //do local points and get local min , max
            int start = ParallelOps.worldProcRank*pointsPerProc;
            int end = pointsPerProc * (ParallelOps.worldProcRank + 1);
            if(ParallelOps.worldProcRank == para - 1){
                end = numPoints;
            }

            //Calc min and max for ecludian distance for mds values
            int count = 0;
            for (int row = start; row < end; row++) {
                count++;
                for (int col = 0; col < numPoints; col++) {
                    double temp = euclideanDist(points[row], points[col]);
                    if(temp > max){
                        max = temp;
                    }

                    if(temp < min){
                        min = temp;
                    }

                }

            }

            max = ParallelOps.allReduceMax(max);
            min = ParallelOps.allReduceMax(min);
            count = ParallelOps.allReduce(count);
            long startPos = ((long)start)*numPoints*Short.BYTES;
            System.out.println("Max : " + max + ":  Min : " + min + ":  Count : " + count);
            ByteBuffer byteBuffer = ByteBuffer.allocate(numPoints*Short.BYTES);
            if(endianness.equals(ByteOrder.BIG_ENDIAN)){
                byteBuffer.order(ByteOrder.BIG_ENDIAN);
            }else{
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            }

            try(RandomAccessFile rmaf = new RandomAccessFile(Paths.get(distanceFile).toString(), "r")) {
                rmaf.seek(startPos);
                byte[] temp = new byte[numPoints*Short.BYTES];
                Buffer buffer = null;
                for (int row = start; row < end; row++) {
                    if(row > 170264) continue;

                    if(ParallelOps.worldProcRank == 0 && row%100 == 0){
                        System.out.println("Row : " + row);
                    }
                    byteBuffer.clear();
                    rmaf.read(temp);
                    byteBuffer.put(temp);
                    byteBuffer.flip();
                    buffer = byteBuffer.asShortBuffer();
                    short[] shortArray = new short[numPoints];
                    ((ShortBuffer)buffer).get(shortArray);
                    for (int col = 0; col < numPoints; col++) {
                        if(col > 170264) continue;
                        //check to make sure correct read
                        if(col == row){
                            if((double)shortArray[col]/Short.MAX_VALUE != 0.0){
                                throw new IllegalStateException("Got incorrect value for diag : " + (double)shortArray[col]/Short.MAX_VALUE);
                            }
                        }

                        double valueOri = (double)shortArray[col]/Short.MAX_VALUE;
                        if(valueOri < 0 || valueOri > 1.0){
                            throw new IllegalStateException("Got incorrect value for value : " + valueOri);
                        }
                        double tempMDS = euclideanDist(points[row], points[col]);
                        double valueMDS = (tempMDS - min) / (max - min);


                        int mdsindex_i = (int) Math.floor(valueMDS * 1000);
                        int orindex_j = (int) Math.floor(valueOri * 1000);

                        if (mdsindex_i == 1000) mdsindex_i -= 1;
                        if (orindex_j == 1000) orindex_j -= 1;

                        histtroOri[orindex_j]++;
                        histtroMDS[mdsindex_i]++;
                        heatmap[mdsindex_i * 1000 + orindex_j] += 1;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            ParallelOps.worldProcsComm.barrier();
            ParallelOps.allReduce(histtroMDS, MPI.SUM, ParallelOps.worldProcsComm);
            ParallelOps.allReduce(histtroOri, MPI.SUM, ParallelOps.worldProcsComm);
            ParallelOps.allReduce(heatmap, MPI.SUM, ParallelOps.worldProcsComm);

            if (ParallelOps.worldProcRank == 0) {

                PrintWriter outWriter = new PrintWriter(new FileWriter(outFileDir + "/" + "heatmap.txt"));
                PrintWriter outWriterhistMds = new PrintWriter(new FileWriter(outFileDir + "/" + "histoMDS.txt"));
                PrintWriter outWriterhistOir = new PrintWriter(new FileWriter(outFileDir + "/" + "histoOri.txt"));

                for (double val : histtroMDS) {
                    outWriterhistMds.print(val+",");
                }

                for (double val : histtroOri) {
                    outWriterhistOir.print(val+",");
                }

                for (int i = 0; i < 1000; i++) {
                    for (int j = 0; j < 1000; j++) {
                        double v = heatmap[i*1000 + j];
                        outWriter.print(v + ",");
                    }
                    outWriter.print("\n");
                }

                outWriter.flush();
                outWriter.close();
                outWriterhistMds.flush();
                outWriterhistMds.close();
                outWriterhistOir.flush();
                outWriterhistOir.close();
            }

            ParallelOps.tearDownParallelism();

        }catch (MPIException e ){
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static double euclideanDist(double[] point1, double[] point2){
        double dist = 0;
        double temp = 0;
        for (int i = 0; i < point1.length; i++) {
            temp = point1[i] - point2[i];
            dist += temp*temp;
        }
        return Math.sqrt(dist);
    }
}
