package org.bitcoin.reader;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.consensusj.bitcoin.jsonrpc.BitcoinClient;
import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BitcoinBlockChainLoader {
    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(BitcoinBlockChainLoader.class);
    private static final AtomicInteger writtenRecordsCounter = new AtomicInteger(0);
    private static final AtomicLong writtenBytesCounter = new AtomicLong(0);

    private static Connection getDatabaseConnection() {
        Connection connection = null;
        try {
            String url = "jdbc:postgresql://localhost:3004/bitcoin";
            // String url = "jdbc:postgresql://marcus-mini.is-very-nice.org:3004/bitcoin";
            String user = "abc";
            String password = "12345";

            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            logger.error("Database connection error: ", e);
        }
        return connection;
    }

    private static Connection refreshDatabaseConnection(Connection conn) throws SQLException {
        try {
            if (conn == null || conn.isClosed()) {
                logger.debug("Refreshing database connection.");
                conn = getDatabaseConnection();
            }
        } catch (SQLException e) {
            logger.error("Error checking or refreshing database connection: ", e);
            throw e;
        }
        return conn;
    }

    public static int getHighestBlock(Connection conn) throws SQLException {
        conn = refreshDatabaseConnection(conn);
        int highestBlock = 1;
        String sql = "SELECT COALESCE(MAX(block_number), 1) FROM transactions_java";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                highestBlock = rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("Error fetching highest block: ", e);
        }

        return highestBlock;
    }

    public static void writeTransactions(Connection conn, List<TransactionJava> transactions) throws SQLException {
        long startTime = System.currentTimeMillis(); // Start metering
        conn = refreshDatabaseConnection(conn);
        if (conn == null || transactions == null || transactions.isEmpty()) {
            throw new SQLException("Connection is null or transactions list is empty.");
        }

        String sql = "INSERT INTO transactions_java (txid, block_number, data, readable_data) VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            long totalBytes = 0;
            for (TransactionJava transaction : transactions) {
                pstmt.setString(1, transaction.getTxid());
                pstmt.setInt(2, transaction.getBlockNumber());
                byte[] data = transaction.getData();
                pstmt.setBytes(3, data);
                pstmt.setString(4, transaction.getReadableData());
                pstmt.addBatch();
                totalBytes += data.length; 
            }
            pstmt.executeBatch();
            writtenRecordsCounter.addAndGet(transactions.size());
            writtenBytesCounter.addAndGet(totalBytes);
            long endTime = System.currentTimeMillis(); // End metering
            logger.debug("writeTransactions executed in " + (endTime - startTime) + " ms, number of transactions: " + transactions.size());
        } catch (SQLException e) {
            logger.error("Error writing transactions: ", e);
            throw e;
        }
    }

    public static List<TransactionJava> getTransactions(BitcoinClient btcCore, int blockNumber) {
        List<TransactionJava> transactions = new ArrayList<>();
        try {
            Sha256Hash blockHash = btcCore.getBlockHash(blockNumber);
            Block block = btcCore.getBlock(blockHash);

            for (Transaction tx : block.getTransactions()) {
                TransactionJava transactionJava = new TransactionJava(tx.getTxId().toString(), blockNumber, tx.serialize(), tx.toString());
                transactions.add(transactionJava);
            }
        } catch (Exception e) {
            logger.error("Error getting transactions for block " + blockNumber + ": ", e);
        }
        return transactions;
    }

    public static void main(String[] args) throws SQLException {
        int x = args.length > 0 ? Integer.parseInt(args[0]) : 1; // Number of BlockReader threads
        int y = args.length > 1 ? Integer.parseInt(args[1]) : 1;  // Number of DBWriter threads
        int maxBatchSize = args.length > 2 ? Integer.parseInt(args[2]) : 10; // Max batch size for DBWriter
        int smallestSize = args.length > 3 ? Integer.parseInt(args[3]) : 20; // Smallest size for DBWriter
        int queueSize = args.length > 4 ? Integer.parseInt(args[4]) : 100; // Queue size for transactionQueue

        BitcoinClient btcCore;
        try {
            btcCore = new BitcoinClient(
                // new URI("http://marcus-mini.is-very-nice.org:3003"),
                new URI("http://localhost:3003"),
                "bitcoinrpc",
                "12345"
            );
        } catch (Exception e) {
            logger.error("Error creating BitcoinClient: ", e);
            return;
        }

        Connection conn = getDatabaseConnection();
        if (conn == null) {
            logger.error("Failed to establish database connection. Exiting.");
            return;
        }

        BlockingQueue<TransactionJava> transactionQueue = new ArrayBlockingQueue<>(queueSize);
        AtomicInteger currentBlockNumber = new AtomicInteger(getHighestBlock(conn));
        // Create x BlockReader threads using ForkJoinPool
        ForkJoinPool blockReaderExecutor = new ForkJoinPool(x);

        for (int i = 0; i < x; i++) {
            blockReaderExecutor.submit(() -> {
                while (true) {
                    int blockNumber = currentBlockNumber.getAndIncrement();
                    List<TransactionJava> transactions = getTransactions(btcCore, blockNumber);
                    try {
                        for (TransactionJava transaction : transactions) {
                            transactionQueue.put(transaction); // Use blocking call
                        }
                        logger.debug("BlockReader - Block number: " + blockNumber + ", Queue size: " + transactionQueue.size() + ", Thread name: " + Thread.currentThread().getName());
                    } catch (InterruptedException e) {
                        logger.error("Error adding transactions to queue: ", e);
                        Thread.currentThread().interrupt(); // Restore interrupted status
                    }
                }
            });
        }

        // Create y DBWriter threads
        ForkJoinPool dbWriterExecutor = new ForkJoinPool(y);

        for (int i = 0; i < y; i++) {
            dbWriterExecutor.submit(() -> {
                while (true) {
                    if (transactionQueue.size() < smallestSize) {
                        try {
                            Thread.sleep(10000); // Sleep for 10 seconds
                            logger.debug("DBWriter - Waiting for more transactions. Current queue size: " + transactionQueue.size() + ", Thread name: " + Thread.currentThread().getName());
                        } catch (InterruptedException e) {
                            logger.error("Error in DBWriter sleep: ", e);
                            Thread.currentThread().interrupt(); // Restore interrupted status
                        }
                    } else {
                        List<TransactionJava> batch = new ArrayList<>();
                        try {
                            transactionQueue.drainTo(batch, maxBatchSize);
                            if (!batch.isEmpty()) {
                                logger.debug("DBWriter - Queue size: " + transactionQueue.size() + ", Batch size: " + batch.size() + ", Thread name: " + Thread.currentThread().getName());
                                writeTransactions(conn, batch);
                            }
                        } catch (Exception e) {
                            logger.error("Error in DBWriter thread: ", e);
                        }
                    }
                }
            });
        }

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
                logger.info("Current Block Number: " + currentBlockNumberValue + ", Queue Size: " + transactionQueue.size() + ", Written Records Change Rate: " + writtenRecordsChangeRate + " per minute, Written MB Change Rate: " + writtenMBChangeRate + " MB per minute");

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
}
