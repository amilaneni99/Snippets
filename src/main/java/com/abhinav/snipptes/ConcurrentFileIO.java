package com.abhinav.snipptes;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Spliterators.spliterator;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.lang3.time.StopWatch.createStarted;

public class ConcurrentFileIO {
    public static void main(String[] args) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        runMultiThreadCsvReadTest();
        System.out.println("Done Reading");
        FileUtils.cleanDirectory(new File("out"));
        long endTime = System.currentTimeMillis();
        System.out.printf("Reading from CSV file took %dms", (endTime-startTime));
    }

    private static void runMultiThreadCsvReadTest() throws InterruptedException, IOException {
        var readAllStopwatch = createStarted();

        var recordsFile = new File("data.csv");
        var csvChunkFiles = splitRecordsFileIntoChunks(recordsFile);

        var executor = newFixedThreadPool(10);
        var latch = new CountDownLatch(10);

        var recordCounts = executor.invokeAll(
                csvChunkFiles.map(f ->
                        (Callable<Integer>)() -> {
                            var count = readCsvFile(f);

                            latch.countDown();

                            return count;
                        }).collect(toList())
        );

        latch.await();
        executor.shutdownNow();

        var totalRecordsRead = recordCounts.stream()
                .mapToInt(rc -> {
                    try {
                        return rc.get();
                    } catch (Throwable ex) {
                        throw new RuntimeException(ex);
                    }
                }).sum();

        readAllStopwatch.stop();

        System.out.printf(
                "Reading %d records from CSV chunk files took %dms%n",
                totalRecordsRead,
                readAllStopwatch.getTime()
        );
    }

    private static Stream<File> splitRecordsFileIntoChunks(File recordsFile) throws IOException {
        var chunkFilesStopwatch = createStarted();
        var chunkFiles = new ArrayList<File>();

        try (
                var fileReader = new FileReader(recordsFile);
                var bufferedReader = new BufferedReader(fileReader)
        ) {
            // open initial file
            var currentFileRecordCount = 0;
            var currentFile = new File(format("out/in_chunk_%d.csv", 0));
            var currentFileWriter = new FileWriter(currentFile);
            var currentBufferedWriter = new BufferedWriter(currentFileWriter);
            var line = bufferedReader.readLine();

            while (nonNull(line)) {
                // dump line into chunk file
                currentBufferedWriter.write(line);
                currentBufferedWriter.newLine();

                currentFileRecordCount++;

                line = bufferedReader.readLine();

                if (nonNull(line) && currentFileRecordCount > 9999) {
                    // open next file if we are at chunk limit and still reading full file
                    currentBufferedWriter.close();
                    currentFileWriter.close();

                    currentBufferedWriter.close();
                    currentFileWriter.close();

                    chunkFiles.add(currentFile);

                    currentFileRecordCount = 0;
                    currentFile = new File(format("out/in_chunk_%d.csv", chunkFiles.size()));
                    currentFileWriter = new FileWriter(currentFile);
                    currentBufferedWriter = new BufferedWriter(currentFileWriter);
                } else if (isNull(line)) {
                    // ensure we add the last file to chunk list
                    chunkFiles.add(currentFile);
                }
            }
        }

        chunkFilesStopwatch.stop();

        System.out.printf(
                "Splitting file '%s' into '%d' chunks of (at most) 10K records each took %dms%n",
                recordsFile.getName(),
                chunkFiles.size(),
                chunkFilesStopwatch.getTime()
        );

        return chunkFiles.stream();
    }

    private static int readCsvFile(File csvFileHandle) {
        var readStopwatch = createStarted();

        try (
                var fileReader = new FileReader(csvFileHandle);
                var csvFile = new CSVParser(fileReader, CSVFormat.EXCEL)
        ) {

            var numRecords = new int[]{0};

            csvFile.iterator().forEachRemaining(record -> {
                System.out.println(record);
                numRecords[0]++;
            });

            readStopwatch.stop();

            System.out.printf(
                    "Reading %d records from CSV file '%s' took %dms%n",
                    numRecords[0],
                    csvFileHandle.getName(),
                    readStopwatch.getTime()
            );
            return numRecords[0];
        } catch (Throwable ex) {
            throw new RuntimeException(
                    format("Error reading from chunk file: %s", csvFileHandle.getName()),
                    ex
            );
        }
    }
}
