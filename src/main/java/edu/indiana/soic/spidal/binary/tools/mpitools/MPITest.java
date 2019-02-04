package edu.indiana.soic.spidal.binary.tools.mpitools;

import mpi.MPI;
import mpi.MPIException;

public class MPITest {

    public static void main(String[] args) throws MPIException {
        MPI.Init(args);

        int myrank = MPI.COMM_WORLD.getRank();
        int size = MPI.COMM_WORLD.getSize() ;
        System.out.println("Hello world from rank " + myrank + " of " + size);

        MPI.Finalize();
    }
}
