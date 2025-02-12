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


// rewrite this class so abstractrwprocessor<T> contructer takes in a list of readers and writers and run them. 
// everything else remain as is
// abstract class reader extends thread
    // read func
// worker extends thread
   // write


abstract public class AbstractRWProcessor<T> {

    protected Logger logger;
    protected String table;
    // protected Connection conn;
    protected AtomicInteger currentBlockNumber;
    protected BlockingQueue<T> recordQueue;
    protected ConcurrentHashMap<String, AtomicInteger> threadWrittenRecordsCounter = new ConcurrentHashMap<>();

    protected Connection refreshDatabaseConnection(Connection conn) throws SQLException {
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
        // this.conn = Utils.getDatabaseConnection(logger);
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

    protected abstract List<T> read(Connection conn, int fromBlockNumber, int toBlockNumber) throws Exception;

    protected abstract void write(Connection conn, List<T> records) throws SQLException;

    protected void writeWithRetry(List<T> records, int maxRetries, long retryDelayMillis) throws SQLException {
        int attempt = 0;
        while (attempt <= maxRetries) {
            Connection connection = null;
            try {
                connection = refreshDatabaseConnection(connection);
                if (connection == null || records == null || records.isEmpty()) {
                    throw new SQLException("Connection is null or records list is empty.");
                }

                // Save original auto-commit setting so we can restore it later
                boolean originalAutoCommit = connection.getAutoCommit();
                try {
                    connection.setAutoCommit(false);
                    write(connection, records);
                    connection.commit();
                    writtenRecordsCounter.addAndGet(records.size());

                    return; // Exit if write is successful
                } catch (SQLException e) {
                    // Roll back in case of an error
                    try {
                        connection.rollback();
                    } catch (SQLException rollbackEx) {
                        logger.error("Error during transaction rollback: ", rollbackEx);
                    }
                    throw e;
                } finally {
                    // Restore the original auto-commit setting
                    connection.setAutoCommit(originalAutoCommit);
                }
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
        ConcurrentHashMap<String, Integer> previousThreadRecords = new ConcurrentHashMap<>();

        while (true) {
            long currentBlockNumberValue = currentBlockNumber.get();
            long currentTime = System.currentTimeMillis();
            long timeElapsed = currentTime - previousTime;

            if (timeElapsed >= 60000) { // 60,000 milliseconds = 1 minute
                int writtenRecordsChangeRate = writtenRecordsCounter.get() - previousWrittenRecords;
                long writtenBytesChangeRate = writtenBytesCounter.get() - previousWrittenBytes;
                double writtenMBChangeRate = writtenBytesChangeRate / (1024.0 * 1024.0);
                logger.info("Block: " + currentBlockNumberValue + 
                            ", Queue: " + recordQueue.size() + 
                            ", Records/min: " + writtenRecordsChangeRate + 
                            ", MB/min: " + writtenMBChangeRate);

                StringBuilder logMessage = new StringBuilder("Thread Records/min: ");
                threadWrittenRecordsCounter.entrySet().stream()
                    .sorted((e1, e2) -> e2.getValue().get() - e1.getValue().get()) // Sort by currentThreadRecords in descending order
                    .forEach(entry -> {
                        String threadName = entry.getKey();
                        int currentThreadRecords = entry.getValue().get();
                        int threadRecordsChangeRate = currentThreadRecords - previousThreadRecords.getOrDefault(threadName, 0);
                        logMessage.append(threadName).append(": ").append(threadRecordsChangeRate).append(", ");
                        previousThreadRecords.put(threadName, currentThreadRecords);
                    });
                logger.info(logMessage.toString());

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

        currentBlockNumber = new AtomicInteger(getHighestBlockNumber(Utils.getDatabaseConnection(logger), table)); // Initialize with a starting block number

        // Create reader threads
        ForkJoinPool readerExecutor = new ForkJoinPool(readerThreads);
        for (int i = 0; i < readerThreads; i++) {
            final int readerIndex = i;
            readerExecutor.submit(() -> {
                Thread.currentThread().setName("RT-" + readerIndex);
                while (true) {
                    int fromBlockNumber = currentBlockNumber.getAndAdd(readBatchSize);
                    int toBlockNumber = fromBlockNumber + readBatchSize; // Example logic to determine the range
                    try (Connection conn = Utils.getDatabaseConnection(logger)) {
                        List<T> records = read(conn, fromBlockNumber, toBlockNumber);
                        for (T record : records) {
                            recordQueue.put(record); // Use blocking call
                        }
                        logger.debug("Reader - From block: " + fromBlockNumber + " to block: " + toBlockNumber + ", Queue: " + recordQueue.size() + ", Thread: " + Thread.currentThread().getName());
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
                String threadName = "WT-" + writerIndex;
                Thread.currentThread().setName(threadName);
                threadWrittenRecordsCounter.put(threadName, new AtomicInteger(0));

                while (true) {
                    if (recordQueue.size() < minBatchSize) {
                        try {
                            Thread.sleep(10000); // Sleep for 10 seconds
                            logger.debug("Writer - Waiting for more records. Queue: " + recordQueue.size() + ", Thread: " + threadName);
                        } catch (InterruptedException e) {
                            logger.error("Error in Writer sleep: ", e);
                            Thread.currentThread().interrupt(); // Restore interrupted status
                        }
                    } else {
                        List<T> batch = new ArrayList<>();
                        try {
                            recordQueue.drainTo(batch, maxBatchSize);
                            if (!batch.isEmpty()) {
                                logger.debug("Writer - Queue: " + recordQueue.size() + ", Batch: " + batch.size() + ", Thread: " + threadName);
                                writeWithRetry(batch, 5, 1000);

                                // Update per-thread counters
                                threadWrittenRecordsCounter.get(threadName).addAndGet(batch.size());
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
