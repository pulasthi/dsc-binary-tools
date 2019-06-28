package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPIException;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class BloombergHeatMap {
    public static void main(String[] args) {
        try {
            ParallelOps.setupParallelism(args);
            Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
            int para = ParallelOps.worldProcsCount;
            int numPoints = Integer.valueOf(args[0]);
            String mdspoints = args[1];
            String fileDir = args[2];
            String outFile = args[3];
            String indexFile = args[4];
            double localMin = Double.MAX_VALUE;
            double localMax = Double.MIN_VALUE;
            double[][] points = new double[numPoints][3];
            Map<Integer, Integer> indexMap = new HashMap<Integer, Integer>();
            int pointsPerProc = numPoints/para;
            ParallelOps.worldProcsComm.barrier();

            double heatmap[] = new double[1000*1000];
            try {
                //read points
                BufferedReader bf = new BufferedReader(new FileReader(mdspoints));
                String line = null;
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


                //read index mapping
                BufferedReader bfin = new BufferedReader(new FileReader(indexFile));
                while ((line = bfin.readLine()) != null){
                    splits = line.split("\\s+");
                    indexMap.put(Integer.valueOf(splits[0]), Integer.valueOf(splits[1]));
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

//            System.out.println("Rank " + ParallelOps.worldProcRank + " S : " + start + " E : " + end);
//            int count = 0;
//            for (int i = start; i < end; i++) {
//                count++;
////                for (double[] point : points) {
////                    double dist = euclideanDist(points[i], point);
////                    localMin = (dist < localMin) ? dist : localMin;
////                    localMax = (dist > localMax) ? dist : localMax;
////                }
//            }

            double max = 2.5511707999080118;
            double min = 0.0;

//            ParallelOps.worldProcsComm.barrier();
//            Utils.printMessage("Done calculations");
//            localMax = ParallelOps.allReduceMax(localMax);
//            Utils.printMessage("Max" + localMax);
//            localMin = ParallelOps.allReduceMin(localMin);
//            Utils.printMessage("Min" + localMin);

            int totalSplits = 192;
            String filePrefirx = "part_";
            int filesPerProc = totalSplits / ParallelOps.worldProcsCount;

            for (int i = 0; i < filesPerProc; i++) {
                int fileIndex = ParallelOps.worldProcRank * filesPerProc + i;
                String fileId = (fileIndex < 100) ? "0" : "";
                fileId += (fileIndex < 10) ? "0" + fileIndex : "" + fileIndex;
                String filePath = fileDir + filePrefirx + fileId;
                BufferedReader bf = new BufferedReader(new FileReader(filePath));
                String line = null;
                String splits[];
                while ((line = bf.readLine()) != null) {
                    splits = line.split("\\s+");

                    int row = indexMap.get(Integer.valueOf(splits[0]));
                    int col = indexMap.get(Integer.valueOf(splits[1]));
                    double valueOri = Double.valueOf(splits[2]);
                    double temp = euclideanDist(points[row], points[col]);
                    double valueMDS = (temp - min) / (max - min);
                    if(valueMDS > 1.0){
                        System.out.println("Error valueMDS " + valueMDS);
                    }

                    if(valueOri > 1.0){
                        System.out.println("Error valueOri " + valueOri);
                    }
                    //convert to indexes
                    int mdsindex_i = (int)Math.floor(valueMDS*1000);
                    int orindex_j = (int)Math.floor(valueOri*1000);

                    heatmap[mdsindex_i*1000 + orindex_j] += 1;
                }
            }

            PrintWriter outWriter = new PrintWriter(new FileWriter(outFile));

            for (int i = 0; i < 1000; i++) {
                for (int j = 0; j < 1000; j++) {
                    double v = heatmap[i*1000 + j];
                    outWriter.print(v + ",");
                }
                outWriter.print("\n");
            }

            outWriter.flush();
            outWriter.close();
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
