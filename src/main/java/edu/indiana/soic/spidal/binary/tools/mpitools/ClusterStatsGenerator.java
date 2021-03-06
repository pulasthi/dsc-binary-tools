package edu.indiana.soic.spidal.binary.tools.mpitools;

import edu.indiana.soic.spidal.common.BinaryReader1D;
import edu.indiana.soic.spidal.common.DoubleStatistics;
import mpi.MPI;
import mpi.MPIException;

import javax.rmi.CORBA.Util;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
            double[] interMax = new double[clustermaprev.size()];
            double[] interMaxAll = new double[clustermaprev.size()];

            double[] intraAverage = new double[clustermaprev.size()*clustermaprev.size()*2];
            double[] intraMin = new double[clustermaprev.size()*clustermaprev.size()];
            double[] intraAverageAll = new double[clustermaprev.size()*clustermaprev.size()*2];
            double[] intraMinAll = new double[clustermaprev.size()*clustermaprev.size()];

            Arrays.fill(interMax, Double.MIN_VALUE);
            Arrays.fill(intraMin, 100.0);


            int globalRow = ParallelOps.procRowStartOffset;
            int curclus;
            int totalClusters = clustermaprev.size();
            //debug
            int cluster15count16 = 0;
            for (int localRow = 0; localRow < ParallelOps.procRowCount; localRow++) {
                globalRow = ParallelOps.procRowStartOffset + localRow;
                curclus = clustermap.get(globalRow);
                for (Integer clusmember : clustermaprev.get(curclus)) {
                    if(clusmember <= globalRow) continue;

                    interAverage[curclus*2] += ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + clusmember]*INV_SHORT_MAX;
                    interAverage[curclus*2 + 1] += 1;

                    if(interMax[curclus] < ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + clusmember]*INV_SHORT_MAX){
                        interMax[curclus] = ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + clusmember]*INV_SHORT_MAX;
                    }
                }
                int colclus;

                for (int col = 0; col < ParallelOps.globalColCount; col++) {
                    if(col <= globalRow) continue;
                    colclus = clustermap.get(col);
                    intraAverage[curclus*totalClusters*2 + colclus*2] += ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + col]*INV_SHORT_MAX;
                    intraAverage[curclus*totalClusters*2 + colclus*2 + 1] += 1;
                    intraAverage[colclus*totalClusters*2 + curclus*2] += ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + col]*INV_SHORT_MAX;
                    intraAverage[colclus*totalClusters*2 + curclus*2 + 1] += 1;
                    //debug
                    // need to add to both sides
                    if(colclus == curclus) {
                        intraMin[colclus*totalClusters + curclus] = 0;
                        intraAverage[colclus*totalClusters*2 + curclus*2] = 0;
                    }

                    if(intraMin[curclus*totalClusters + colclus] > ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + col]*INV_SHORT_MAX &&
                            ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + col]*INV_SHORT_MAX > 0){
                        intraMin[curclus*totalClusters + colclus] = ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + col]*INV_SHORT_MAX;
                        intraMin[colclus*totalClusters + curclus] = ParallelOps.PointDistances[localRow*ParallelOps.globalColCount + col]*INV_SHORT_MAX;
                    }



                }
            }
            //if(intraMin[66*totalClusters + 5] == 0.0) System.out.println("Got a zero here" + ParallelOps.worldProcRank);

            ParallelOps.allReduceBuff(interAverage, MPI.SUM, interAverageAll);
            ParallelOps.allReduceBuff(interMax, MPI.MAX, interMaxAll);
            ParallelOps.allReduceBuff(intraAverage, MPI.SUM, intraAverageAll);
            ParallelOps.allReduceBuff(intraMin, MPI.MIN, intraMinAll);

            int all1516 = ParallelOps.allReduce(cluster15count16);
            Utils.printMessage("The total number of distance taken in cluster 15 : " + all1516);
            for (int i = 0; i < interMaxAll.length; i++) {
                Utils.printMessage(String.format("Cluster %d : Max : %.14f", i,interMaxAll[i]));
                Utils.printMessage(String.format("Cluster %d and %d : Min : %.10f", i,0,intraMinAll[i*totalClusters]));
                Utils.printMessage(String.format("0 Cluster %d and %d : Min : %.10f", i,0,intraMin[i*totalClusters]));
            }

            Utils.printMessage("symetry chkeck" +  (intraMinAll[2] == intraMinAll[2*totalClusters]));
            Utils.printMessage("symetry chkeck" +  (intraMinAll[12*totalClusters + 2] == intraMinAll[2*totalClusters+12]));

            //Write the results to a file
            if(ParallelOps.worldProcRank == 0){
                PrintWriter printWriterintra = new PrintWriter(new FileWriter(outFile + "_intraa.csv"));
                PrintWriter printWriterinterAv= new PrintWriter(new FileWriter(outFile + "_interAverage.csv"));
                PrintWriter printWriterinterMin = new PrintWriter(new FileWriter(outFile + "_interMin.csv"));

                //Inter cluster
                printWriterintra.println("Cluster,Average,Max");
                for (int i = 0; i < totalClusters; i++) {
                    if(i > 210) continue; // do not need dust and referance points
                    printWriterintra.printf("%d,%.6f,%.6f \n", i, interAverageAll[i*2]/interAverageAll[i*2 + 1], interMaxAll[i]);
                }

                //Intra average
                for (int i = 0; i < totalClusters; i++) {
                    if(i > 210) continue; // do not need dust and referance points
                    for (int j = 0; j < totalClusters; j++) {
                        if(j > 210) continue; // do not need dust and referance points
                        printWriterinterAv.print(intraAverageAll[i*totalClusters*2 + j*2]/intraAverageAll[i*totalClusters*2 + j*2 + 1] + ",");
                    }
                    printWriterinterAv.println();
                }

                //Intra Min
                for (int i = 0; i < totalClusters; i++) {
                    if(i > 210) continue; // do not need dust and referance points
                    for (int j = 0; j < totalClusters; j++) {
                        if(j > 210) continue; // do not need dust and referance points
                        printWriterinterMin.print(intraMinAll[i*totalClusters + j] + ",");
                    }
                    printWriterinterMin.println();
                }

                printWriterinterAv.close();
                printWriterinterMin.close();
                printWriterintra.close();
            }

            //output statas
            calculateDistStats();

            ParallelOps.tearDownParallelism();
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
