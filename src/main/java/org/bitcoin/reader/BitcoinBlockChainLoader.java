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

public class BitcoinBlockChainLoader {
    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(BitcoinBlockChainLoader.class);
    private static final AtomicInteger writtenRecordsCounter = new AtomicInteger(0);

    private static Connection getDatabaseConnection() {
        Connection connection = null;
        try {

            String url = "jdbc:postgresql://localhost:3004/bitcoin";
        //     String url = "jdbc:postgresql://marcus-mini.is-very-nice.org:3004/bitcoin";
            String user = "abc";
            String password = "12345";

            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            logger.error("Database connection error: ", e);
        }
        return connection;
    }

    private static Connection refreshDatabaseConnection(Connection conn) {
        try {
            if (conn == null || conn.isClosed()) {
                logger.info("Refreshing database connection.");
                conn = getDatabaseConnection();
            }
        } catch (SQLException e) {
            logger.error("Error checking or refreshing database connection: ", e);
        }
        return conn;
    }

    public static int getHighestBlock(Connection conn) {
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

    public static Boolean writeTransactions(Connection conn, List<TransactionJava> transactions) {
        long startTime = System.currentTimeMillis(); // Start metering
        conn = refreshDatabaseConnection(conn);
        if (conn == null || transactions == null || transactions.isEmpty()) {
            return false;
        }

        String sql = "INSERT INTO transactions_java (txid, block_number, data, readable_data) VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (TransactionJava transaction : transactions) {
                pstmt.setString(1, transaction.getTxid());
                pstmt.setInt(2, transaction.getBlockNumber());
                pstmt.setBytes(3, transaction.getData());
                pstmt.setString(4, transaction.getReadableData());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            writtenRecordsCounter.addAndGet(transactions.size());
            long endTime = System.currentTimeMillis(); // End metering
            logger.info("writeTransactions executed in " + (endTime - startTime) + " ms");
            return true;
        } catch (SQLException e) {
            logger.error("Error writing transactions: ", e);
            return false;
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

    public static void main(String[] args) {
        int x = args.length > 0 ? Integer.parseInt(args[0]) : 50; // Number of BlockReader threads
        int y = args.length > 1 ? Integer.parseInt(args[1]) : 10;  // Number of DBWriter threads
        int batchSize = args.length > 2 ? Integer.parseInt(args[2]) : 1000; // Batch size for DBWriter

        BitcoinClient btcCore;
        try {
            btcCore = new BitcoinClient(
                //     new URI("http://marcus-mini.is-very-nice.org:3003"),
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

        BlockingQueue<TransactionJava> transactionQueue = new ArrayBlockingQueue<>(10000);
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
                    if (transactionQueue.size() < batchSize) {
                        try {
                            Thread.sleep(10000); // Sleep for 10 seconds
                            logger.info("DBWriter - Waiting for more transactions. Current queue size: " + transactionQueue.size() + ", Thread name: " + Thread.currentThread().getName());
                        } catch (InterruptedException e) {
                            logger.error("Error in DBWriter sleep: ", e);
                            Thread.currentThread().interrupt(); // Restore interrupted status
                        }
                    } else {
                        List<TransactionJava> batch = new ArrayList<>();
                        try {
                            transactionQueue.drainTo(batch, batchSize * 2);
                            if (!batch.isEmpty()) {
                                logger.info("DBWriter - Queue size: " + transactionQueue.size() + ", Batch size: " + batch.size() + ", Thread name: " + Thread.currentThread().getName());
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

        while (true) {
            long currentBlockNumberValue = currentBlockNumber.get();
            long currentTime = System.currentTimeMillis();
            long timeElapsed = currentTime - previousTime;

            if (timeElapsed >= 60000) { // 60,000 milliseconds = 1 minute
                int writtenRecordsChangeRate = writtenRecordsCounter.get() - previousWrittenRecords;
                logger.info("Current Block Number: " + currentBlockNumberValue + ", Queue Size: " + transactionQueue.size() + ", Written Records Change Rate: " + writtenRecordsChangeRate + " per minute");

                previousBlockNumber = currentBlockNumberValue;
                previousTime = currentTime;
                previousWrittenRecords = writtenRecordsCounter.get();
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
