package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPIException;

public class BloombergSparseGen {
    public static void main(String[] args) {
        try {
            ParallelOps.setupParallelism(args);

        Utils.printMessage("Starting with " + ParallelOps.worldProcsCount + "Processes");
        String fileDir = args[0];

            int totalSplits = 48;
            String filePrefirx = "part_";
            int filesPerProc = totalSplits / ParallelOps.worldProcsCount;

            //Min and max of all the files, this was calculated using the stats calculation
            double min = 0;
            double max = 0;

            


        } catch (MPIException e) {
            e.printStackTrace();
        }
    }
}
