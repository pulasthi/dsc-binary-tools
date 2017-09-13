#!/bin/bash
#edit the input properties file to the cluster extractor
binFile=$1
outFile=$2
numPoints=$3
range=$4
endian=$5
dataType=$6
sample=$7
sampleCount=$8

cp=$HOME/.m2/repository/dsc-binary-tools/dsc-binary-tools/1.0-SNAPSHOT/dsc-binary-tools-1.0-SNAPSHOT-jar-with-dependencies.jar
java -cp $cp edu.indiana.soic.spidal.binary.tools.HistogramGenerator $binFile $outFile $numPoints $range $endian $dataType $sample $sampleCount
