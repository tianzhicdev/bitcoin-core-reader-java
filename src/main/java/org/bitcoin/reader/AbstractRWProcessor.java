package org.bitcoin.reader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.Logger;

abstract public class AbstractRWProcessor<T> {

    protected Logger logger;
    protected String table;
    protected Connection conn;
    protected AtomicInteger currentBlockNumber;
    protected BlockingQueue<T> recordQueue;
    protected ConcurrentHashMap<String, AtomicInteger> threadWrittenRecordsCounter = new ConcurrentHashMap<>();
    protected ConcurrentHashMap<String, AtomicLong> threadWrittenBytesCounter = new ConcurrentHashMap<>();

    protected Connection refreshDatabaseConnection() throws SQLException {
        try {
            if (conn == null || conn.isClosed()) {
                logger.debug("Refreshing database connection.");
                conn = Utils.getDatabaseConnection(logger);
            }
        } catch (SQLException e) {
            logger.error("Error checking or refreshing database connection: ", e);
            throw e;
        }
        return conn;
    }

    public AbstractRWProcessor(Logger logger, String table) throws SQLException {
        this.logger = logger;
        this.table = table;
        this.conn = Utils.getDatabaseConnection(logger);
    }

    protected final AtomicInteger writtenRecordsCounter = new AtomicInteger(0);
    protected final AtomicLong writtenBytesCounter = new AtomicLong(0);

    protected int getHighestBlockNumber(Connection conn, String tableName) throws SQLException {
        int highestBlock = 1;
        String sql = "SELECT COALESCE(MAX(block_number), 1) FROM " + tableName;

        try (PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                highestBlock = rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("Error fetching highest block from " + tableName + ": ", e);
            throw e;
        }

        return highestBlock;
    }

    protected abstract List<T> read(int fromBlockNumber, int toBlockNumber) throws Exception;

    protected abstract void write(List<T> records) throws SQLException;

    protected void writeWithRetry(List<T> records, int maxRetries, long retryDelayMillis) throws SQLException {
        int attempt = 0;
        while (attempt <= maxRetries) {
            try {
                write(records);
                return; // Exit if write is successful
            } catch (SQLException e) {
                attempt++;
                if (attempt > maxRetries) {
                    logger.error("Max retries reached. Failed to write records: ", e);
                    throw e;
                }
                logger.warn("Write attempt " + attempt + " failed. Retrying in " + retryDelayMillis + "ms.", e);
                try {
                    Thread.sleep(retryDelayMillis);
                } catch (InterruptedException ie) {
                    logger.error("Retry sleep interrupted: ", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    protected void periodicallyReport() {
        long previousTime = System.currentTimeMillis();
        int previousWrittenRecords = writtenRecordsCounter.get();
        long previousWrittenBytes = writtenBytesCounter.get();

        while (true) {
            long currentBlockNumberValue = currentBlockNumber.get();
            long currentTime = System.currentTimeMillis();
            long timeElapsed = currentTime - previousTime;

            if (timeElapsed >= 60000) { // 60,000 milliseconds = 1 minute
                int writtenRecordsChangeRate = writtenRecordsCounter.get() - previousWrittenRecords;
                long writtenBytesChangeRate = writtenBytesCounter.get() - previousWrittenBytes;
                double writtenMBChangeRate = writtenBytesChangeRate / (1024.0 * 1024.0);
                logger.info("Block Number: " + currentBlockNumberValue + 
                            "\nQueue Size: " + recordQueue.size() + 
                            "\nRecords Change Rate: " + writtenRecordsChangeRate + " records/min" + 
                            "\nData Change Rate: " + writtenMBChangeRate + " MB/min");

                for (String threadName : threadWrittenRecordsCounter.keySet()) {
                    int threadWrittenRecords = threadWrittenRecordsCounter.get(threadName).get();
                    long threadWrittenBytes = threadWrittenBytesCounter.get(threadName).get();
                    double threadWrittenMB = threadWrittenBytes / (1024.0 * 1024.0);
                    logger.info("Thread: " + threadName + 
                                "\nRecords Written: " + threadWrittenRecords + 
                                "\nData Written: " + threadWrittenMB + " MB");
                }

                previousTime = currentTime;
                previousWrittenRecords = writtenRecordsCounter.get();
                previousWrittenBytes = writtenBytesCounter.get();
            }

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                logger.error("Error in logging thread: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    public void execute(int readerThreads, int writerThreads, int queueSize, int readBatchSize, int minBatchSize, int maxBatchSize) throws SQLException {
        logger.info("Executing with parameters: " +
                    "Reader Threads: " + readerThreads + ", " +
                    "Writer Threads: " + writerThreads + ", " +
                    "Queue Size: " + queueSize + ", " +
                    "Read Batch Size: " + readBatchSize + ", " +
                    "Min Batch Size: " + minBatchSize + ", " +
                    "Max Batch Size: " + maxBatchSize);
        recordQueue = new ArrayBlockingQueue<>(queueSize);
        currentBlockNumber = new AtomicInteger(getHighestBlockNumber(conn, table)); // Initialize with a starting block number

        // Create reader threads
        ForkJoinPool readerExecutor = new ForkJoinPool(readerThreads);
        for (int i = 0; i < readerThreads; i++) {
            final int readerIndex = i;
            readerExecutor.submit(() -> {
                Thread.currentThread().setName("Reader-Thread-" + readerIndex);
                while (true) {
                    int fromBlockNumber = currentBlockNumber.getAndAdd(readBatchSize);
                    int toBlockNumber = fromBlockNumber + readBatchSize; // Example logic to determine the range
                    try {
                        List<T> records = read(fromBlockNumber, toBlockNumber);
                        for (T record : records) {
                            recordQueue.put(record); // Use blocking call
                        }
                        logger.debug("Reader - From block number: " + fromBlockNumber + " to block number: " + toBlockNumber + ", Queue size: " + recordQueue.size() + ", Thread name: " + Thread.currentThread().getName());
                    } catch (Exception e) {
                        logger.error("Error reading records for block range " + fromBlockNumber + " to " + toBlockNumber + ": ", e);
                    }
                }
            });
        }

        // Create writer threads
        ForkJoinPool writerExecutor = new ForkJoinPool(writerThreads);
        for (int i = 0; i < writerThreads; i++) {
            final int writerIndex = i;
            writerExecutor.submit(() -> {
                String threadName = "Writer-Thread-" + writerIndex;
                Thread.currentThread().setName(threadName);
                threadWrittenRecordsCounter.put(threadName, new AtomicInteger(0));
                threadWrittenBytesCounter.put(threadName, new AtomicLong(0));

                while (true) {
                    if (recordQueue.size() < minBatchSize) {
                        try {
                            Thread.sleep(10000); // Sleep for 10 seconds
                            logger.debug("Writer - Waiting for more records. Current queue size: " + recordQueue.size() + ", Thread name: " + threadName);
                        } catch (InterruptedException e) {
                            logger.error("Error in Writer sleep: ", e);
                            Thread.currentThread().interrupt(); // Restore interrupted status
                        }
                    } else {
                        List<T> batch = new ArrayList<>();
                        try {
                            recordQueue.drainTo(batch, maxBatchSize);
                            if (!batch.isEmpty()) {
                                logger.debug("Writer - Queue size: " + recordQueue.size() + ", Batch size: " + batch.size() + ", Thread name: " + threadName);
                                writeWithRetry(batch, 5, 1000);

                                // Update per-thread counters
                                threadWrittenRecordsCounter.get(threadName).addAndGet(batch.size());
                                long batchBytes = batch.stream().mapToLong(record -> ((byte[]) record).length).sum(); // Assuming T is byte[]
                                threadWrittenBytesCounter.get(threadName).addAndGet(batchBytes);
                            }
                        } catch (Exception e) {
                            logger.error("Error in Writer thread: ", e);
                        }
                    }
                }
            });
        }

        periodicallyReport();
    }
}
