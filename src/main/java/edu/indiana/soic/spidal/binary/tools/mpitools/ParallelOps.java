package edu.indiana.soic.spidal.binary.tools.mpitools;

import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.indiana.soic.spidal.common.Range;
import edu.indiana.soic.spidal.common.RangePartitioner;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Op;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class ParallelOps {
    public static String machineName;
    public static int nodeCount=1;
    public static int threadCount=1;

    public static int nodeId;

    public static Intracomm worldProcsComm;
    public static int worldProcRank;
    public static int worldProcsCount;
    public static int worldProcsPerNode;
    public static int mmapProcsCount;
    public static boolean isMmapLead;


    // mmap leaders form one communicating group and the others (followers)
    // belong to another communicating group.
    public static Intracomm cgProcComm;
    public static int cgProcRank;
    public static int[] cgProcsMmapXByteExtents;
    public static int[] cgProcsMmapXDisplas;

    public static String parallelPattern;
    public static Range[] procRowRanges;
    public static Range procRowRange;
    public static int procRowStartOffset;
    public static int procRowCount;

    public static Range[] threadRowRanges;
    public static int[] threadRowStartOffsets;
    public static int[] threadRowCounts;

    public static int globalColCount;

    // Buffers for MPI operations
    private static ByteBuffer statBuffer;
    private static DoubleBuffer doubleBuffer;
    private static IntBuffer intBuffer;

    public static Bytes mmapLockOne;
    public static Bytes mmapEntryLock;
    private static int FLAG = 0;
    private static int COUNT = Long.BYTES;

    public static ByteBuffer mmapXReadByteBuffer;
    public static ByteBuffer fullXByteBuffer;

    public static ByteBuffer mmapSReadByteBuffer;

    public static short[] PointDistances; // 1st dimension is rowIdx, 2nd is colIdx

    public static void setupParallelism(String[] args) throws MPIException {
        MPI.Init(args);
        machineName = MPI.getProcessorName();

        /* Allocate basic buffers for communication */
        statBuffer = MPI.newByteBuffer(DoubleStatistics.extent);
        doubleBuffer = MPI.newDoubleBuffer(4000);
        intBuffer = MPI.newIntBuffer(1);

        worldProcsComm = MPI.COMM_WORLD; //initializing MPI world communicator
        worldProcRank = worldProcsComm.getRank();
        worldProcsCount = worldProcsComm.getSize();

        /* Create communicating groups */
        worldProcsPerNode = worldProcsCount / nodeCount;
        boolean heterogeneous = (worldProcsPerNode * nodeCount) != worldProcsCount;
        if (heterogeneous) {
            Utils.printMessage("Running in heterogeneous mode");
        }

        parallelPattern =
                "---------------------------------------------------------\n"
                        + "Machine:" + machineName + ' ' + threadCount + 'x'
                        + worldProcsPerNode + 'x' + nodeCount;
        Utils.printMessage(parallelPattern);
    }

    public static void tempBreak() throws MPIException {
        if (worldProcRank ==0){
            MPI.Finalize();
            System.exit(0);
        } else {
            MPI.Finalize();
            System.exit(0);
        }
    }

    public static void tearDownParallelism() throws MPIException {
        // End MPI
        MPI.Finalize();
    }

    public static void setParallelDecomposition(int globalRowCount)
            throws IOException, MPIException {
        //	First divide points among processes
        procRowRanges = RangePartitioner.partition(globalRowCount,
                worldProcsCount);
        Range rowRange = procRowRanges[worldProcRank]; // The range of points for this process

        procRowRange = rowRange;
        procRowStartOffset = rowRange.getStartIndex();
        procRowCount = rowRange.getLength();
        globalColCount = globalRowCount;
        PointDistances = new short[procRowCount*globalRowCount];


        // Next partition points per process among threads
        threadRowRanges = RangePartitioner.partition(procRowCount, threadCount);
        threadRowCounts = new int[threadCount];
        threadRowStartOffsets = new int[threadCount];

        IntStream.range(0, threadCount)
                .parallel()
                .forEach(threadIdx -> {
                    Range threadRowRange = threadRowRanges[threadIdx];
                    threadRowCounts[threadIdx] =
                            threadRowRange.getLength();
                    threadRowStartOffsets[threadIdx] =
                            threadRowRange.getStartIndex();
                });

    }

    public static DoubleStatistics allReduce(DoubleStatistics stat)
            throws MPIException {
        stat.addToBuffer(statBuffer, 0);
        worldProcsComm.allReduce(statBuffer, DoubleStatistics.extent, MPI.BYTE,
                DoubleStatistics.reduceSummaries());
        return DoubleStatistics.getFromBuffer(statBuffer, 0);
    }

    public static double allReduce(double value) throws MPIException{
        doubleBuffer.put(0, value);
        worldProcsComm.allReduce(doubleBuffer, 1, MPI.DOUBLE, MPI.SUM);
        return doubleBuffer.get(0);
    }

    public static double allReduceMax(double value) throws MPIException{
        doubleBuffer.put(0, value);
        worldProcsComm.allReduce(doubleBuffer, 1, MPI.DOUBLE, MPI.MAX);
        return doubleBuffer.get(0);
    }

    public static double allReduceMin(double value) throws MPIException{
        doubleBuffer.put(0, value);
        worldProcsComm.allReduce(doubleBuffer, 1, MPI.DOUBLE, MPI.MIN);
        return doubleBuffer.get(0);
    }

    public static int allReduce(int value) throws MPIException{
        intBuffer.put(0, value);
        worldProcsComm.allReduce(intBuffer, 1, MPI.INT, MPI.SUM);
        return intBuffer.get(0);
    }

    public static void allReduce(double [] values, Op reduceOp, Intracomm comm) throws MPIException {
        comm.allReduce(values, values.length, MPI.DOUBLE, reduceOp);
    }

    public static  void allReduceBuff(double [] values, Op reduceOp, double[] result) throws MPIException{
        allReduceBuff(values, reduceOp, worldProcsComm, result);
    }

    public static void allReduceBuff(double [] values, Op reduceOp, Intracomm comm,  double[] result) throws MPIException {
        doubleBuffer.clear();
        doubleBuffer.put(values,0,values.length);
        comm.allReduce(doubleBuffer, values.length, MPI.DOUBLE, reduceOp);
        doubleBuffer.flip();
        doubleBuffer.get(result);
    }

    public static void allGather() throws MPIException {

        /* Safety logic to make sure all procs in the mmap has reached here. Otherwise, it's possible that
            * one (or more) procs from a same mmap may have come here while a previous call to this collective
            * is being carried out by the other procs in the same mmap. Also, note the use of a separate lock for this,
            * without that there's a chance to crash/hang */
        if (mmapEntryLock.addAndGetInt(COUNT, 1) == mmapProcsCount){
            mmapEntryLock.writeInt(COUNT, 0);
        } else {
            int count;
            do  {
                count = mmapEntryLock.readInt(COUNT);
            } while (count != 0);
        }

        if (ParallelOps.isMmapLead) {
            cgProcComm.allGatherv(mmapXReadByteBuffer,
                    cgProcsMmapXByteExtents[cgProcRank], MPI.BYTE,
                    fullXByteBuffer, cgProcsMmapXByteExtents,
                    cgProcsMmapXDisplas, MPI.BYTE);

            if (mmapProcsCount > 1) {
                mmapLockOne.writeInt(COUNT, 1); // order matters as no locks
                mmapLockOne.writeBoolean(FLAG, true);
            } else {
                /* This is for the case if you only have 1 proc per mmap,
                * then it needs to clear the flag and reset the count.
                * We special case when 1 proc per mmap under uniform mode, but
                * in a heterogeneous setting it's possible to have an mmap with 1 proc, hence this logic*/
                mmapLockOne.writeInt(COUNT, 0); // order does NOT matter for this case
                mmapLockOne.writeBoolean(FLAG, false);
            }
        } else {
            busyWaitTillDataReady();
        }
    }

    private static void busyWaitTillDataReady(){
        boolean ready = false;
        int count;
        while (!ready){
            ready = mmapLockOne.readBoolean(FLAG);
        }
        count = mmapLockOne.addAndGetInt(COUNT,1);
        if (count == mmapProcsCount){
            mmapLockOne.writeBoolean(FLAG, false);
            mmapLockOne.writeInt(COUNT, 0);
        }
    }

    public static void partialXAllGather() throws MPIException {
        cgProcComm.allGatherv(mmapXReadByteBuffer,
                cgProcsMmapXByteExtents[cgProcRank], MPI.BYTE,
                fullXByteBuffer, cgProcsMmapXByteExtents,
                cgProcsMmapXDisplas, MPI.BYTE);
    }

    public static void partialSAllReduce(Op op) throws MPIException{
        cgProcComm.allReduce(mmapSReadByteBuffer, 1, MPI.DOUBLE,op);
    }

    public static void broadcast(ByteBuffer buffer, int extent, int root)
            throws MPIException {
        worldProcsComm.bcast(buffer, extent, MPI.BYTE, root);
    }

    public static void gather(LongBuffer buffer, int count, int root)
            throws MPIException {
        worldProcsComm.gather(buffer, count, MPI.LONG, root);
    }
}