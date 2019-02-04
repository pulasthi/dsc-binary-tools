package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPI;
import mpi.MPIException;

public class MPITest {

    public static void main(String[] args) throws MPIException {
        MPI.Init(args);

        int rank = MPI.COMM_WORLD.getRank(),
                size = MPI.COMM_WORLD.getSize(),
                nint = 100; // Intervals.
        double h = 1.0/(double)nint, sum = 0.0;

        for(int i=rank+1; i<=nint; i+=size) {
            double x = h * ((double)i - 0.5);
            sum += (4.0 / (1.0 + x * x));
        }

        double sBuf[] = { h * sum },
                rBuf[] = new double[1];

        MPI.COMM_WORLD.reduce(sBuf, rBuf, 1, MPI.DOUBLE, MPI.SUM, 0);

        if(rank == 0) System.out.println("PI: " + rBuf[0]);
        MPI.Finalize();
    }
}
