package edu.indiana.soic.spidal.binary.tools.mpitools;

import edu.indiana.soic.spidal.common.BinaryReader1D;
import edu.indiana.soic.spidal.common.DoubleStatistics;
import mpi.MPI;
import mpi.MPIException;

import javax.rmi.CORBA.Util;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Generates average, max, min distances for inter and intra clusters for a given distance bin file and
 * a cluster file
 */
public class ClusterStatsGenerator {
    public static ByteOrder endianness;
    public static final double INV_SHORT_MAX = 1.0 / Short.MAX_VALUE;

    public static void main(String[] args) {
        try{
            ParallelOps.setupParallelism(args);
            Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
            String distanceFile = args[0];
            String clusterFile = args[1];
            String outFile = args[2];
            int numPoints = Integer.valueOf(args[3]);
            endianness =  args[4].equals("big") ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

            ParallelOps.setParallelDecomposition(numPoints);

            //read clusters and store in Hashmap
            Map<Integer, Integer> clustermap = new HashMap<Integer, Integer>();
            Map<Integer, ArrayList<Integer>> clustermaprev = new HashMap<Integer, ArrayList<Integer>>();
            readClusterFile(clusterFile,clustermap, clustermaprev);
            //read data from bin file
            readDistanceData(distanceFile);

            //Data structures to keep calculations
            double[] interAverage = new double[clustermaprev.size()*2];
            double[] interAverageAll = new double[clustermaprev.size()*2];
            double[] inteMax = new double[clustermaprev.size()];
            double[] inteMaxAll = new double[clustermaprev.size()];
            Arrays.fill(inteMax, Double.MIN_VALUE);


            int globalRow = ParallelOps.procRowStartOffset;
            int curclus;
            //debug
            int cluster15count = 0;
            for (int localRow = 0; localRow < ParallelOps.procRowCount; localRow++) {
                globalRow = ParallelOps.procRowStartOffset + localRow;
                curclus = clustermap.get(globalRow);
                for (Integer clusmember : clustermaprev.get(curclus)) {
                    if(clusmember <= globalRow) continue;

                    interAverage[curclus*2] += ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + clusmember];
                    interAverage[curclus*2 + 1] += 1;

                    if(inteMax[curclus] < ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + clusmember]){
                        inteMax[curclus] = ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + clusmember];
                    }
                    //debug
                    if(curclus == 15) cluster15count++;


                }
            }

            ParallelOps.allReduceBuff(interAverage, MPI.SUM, interAverageAll);
            ParallelOps.allReduceBuff(inteMax, MPI.MAX, inteMaxAll);
            int all15 = ParallelOps.allReduce(cluster15count);
            Utils.printMessage("The total number of distance taken in cluster 15 : " + all15);
            for (int i = 0; i < inteMaxAll.length; i++) {
                Utils.printMessage(String.format("Cluster %d : Max : %.4f", i,inteMax[i]));
            }

            //output statas
            calculateDistStats();

        } catch (MPIException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void calculateDistStats() throws MPIException {
        double localCount = ParallelOps.PointDistances.length;
        double localSum = 0.0;
        double localSum2 = 0.0;
        double localMax = Double.MIN_VALUE;
        double origD = 0.0;
        for (int i = 0; i < localCount; i++) {
            origD = ParallelOps.PointDistances[i]*INV_SHORT_MAX;
            if(origD > 0) localSum += origD;
            if(origD > localMax){
                localMax = origD;
            }
        }
        for (int i = 0; i < ParallelOps.procRowCount; i++) {
            for (int j = 0; j < ParallelOps.globalColCount; j++) {
                origD = ((double)ParallelOps.PointDistances[i*ParallelOps.globalColCount + j])*INV_SHORT_MAX;
                localSum2 += origD;
            }
        }

        double totalCount = ParallelOps.allReduce(localCount);
        double totalSum = ParallelOps.allReduce(localSum);
        double totalSum2 = ParallelOps.allReduce(localSum2);
        double max = ParallelOps.allReduceMax(localMax);
        Utils.printMessage(String.format(" The total count : %.10f \n TotalSum1 : %.10f \n TotalSum2 : %.10f \n Max :  %.10f", totalCount, totalSum, totalSum2, max ));
    }

    private static void readDistanceData(String distanceFile) {

        BinaryReader1D.readRowRange(distanceFile, ParallelOps.procRowRange,
                ParallelOps.globalColCount, endianness, true,
                null, ParallelOps.PointDistances);
    }

    private static void readClusterFile(String clusterFile, Map<Integer, Integer> clustermap, Map<Integer, ArrayList<Integer>> clustermapRev) {
        String line = null;
        Pattern pattern = Pattern.compile("[\t]");
        try(BufferedReader br = Files.newBufferedReader(Paths.get(clusterFile))){
            while ((line = br.readLine()) != null){
                String[] splits = pattern.split(line);
                int index = Integer.valueOf(splits[0]);
                int cluster = Integer.valueOf(splits[1]);
                clustermap.put(index,cluster);
                if(!clustermapRev.containsKey(cluster)) clustermapRev.put(cluster, new ArrayList<Integer>());
                clustermapRev.get(cluster).add(index);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
