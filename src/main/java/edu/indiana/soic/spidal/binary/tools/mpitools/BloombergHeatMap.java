package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPIException;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;

public class BloombergHeatMap {
    public static void main(String[] args) {
        try {
            ParallelOps.setupParallelism(args);
            Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
            int para = ParallelOps.worldProcsCount;
            int numPoints = Integer.valueOf(args[0]);
            String mdspoints = args[1];
            double localMin = Double.MAX_VALUE;
            double localMax = Double.MIN_VALUE;
            double[][] points = new double[numPoints][3];
            int pointsPerProc = numPoints/para;

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


            }catch (IOException e){
                e.printStackTrace();
            }

            //do local points and get local min , max
            int start = ParallelOps.worldProcRank*pointsPerProc;
            int end = pointsPerProc * (ParallelOps.worldProcRank + 1);
            if(ParallelOps.worldProcRank == para - 1){
                end = numPoints;
            }

            for (int i = start; i < end; i++) {
                for (double[] point : points) {
                    double dist = euclideanDist(points[i], point);
                    localMin = (dist < localMin) ? dist : localMin;
                    localMax = (dist > localMax) ? dist : localMax;
                }
                if(i%100 == 0){
                    Utils.printMessage(".");
                }
            }

            double max = ParallelOps.allReduceMax(localMax);
            double min = ParallelOps.allReduceMin(localMin);
            System.out.println("Min" + min);
            System.out.println("Max" + max);

        }catch (MPIException e){
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
