package edu.indiana.soic.spidal.binary.tools.mpitools;

import edu.indiana.soic.spidal.common.BinaryReader1D;
import edu.indiana.soic.spidal.common.DoubleStatistics;
import mpi.MPIException;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
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
            readClusterFile(clusterFile,clustermap);
            //read data from bin file

            //Data structures to keep calculations
            readDistanceData(distanceFile);

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

    private static void readClusterFile(String clusterFile, Map<Integer, Integer> clustermap) {
        String line = null;
        Pattern pattern = Pattern.compile("[\t]");
        try(BufferedReader br = Files.newBufferedReader(Paths.get(clusterFile))){
            while ((line = br.readLine()) != null){
                String[] splits = pattern.split(line);
                clustermap.put(Integer.valueOf(splits[0]),Integer.valueOf(splits[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
