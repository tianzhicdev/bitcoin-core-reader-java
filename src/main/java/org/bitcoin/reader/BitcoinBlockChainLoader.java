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

    static private Connection getDatabaseConnection() {
        Connection connection = null;
        try {
            String url = "jdbc:postgresql://marcus-mini.is-very-nice.org:3004/bitcoin";
            String user = "abc";
            String password = "12345";

            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            logger.error("Database connection error: ", e);
        }
        return connection;
    }

    static public int getHighestBlock(Connection conn) {
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

    static public Boolean writeTransactions(Connection conn, List<TransactionJava> transactions) {
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
            return true;
        } catch (SQLException e) {
            logger.error("Error writing transactions: ", e);
            return false;
        }
    }

    static public List<TransactionJava> getTransactions(BitcoinClient btcCore, int blockNumber) {
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

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: java BitcoinBlockChainLoader <numBlockReaderThreads> <numDBWriterThreads> <batchSize>");
            System.exit(1);
        }

        int x = Integer.parseInt(args[0]); // Number of BlockReader threads
        int y = Integer.parseInt(args[1]); // Number of DBWriter threads
        int batchSize = Integer.parseInt(args[2]); // Batch size for DBWriter

        BitcoinClient btcCore = new BitcoinClient(
                new URI("http://marcus-mini.is-very-nice.org:3003"),
                "bitcoinrpc",
                "12345"
        );

        Connection conn = getDatabaseConnection();

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
                        transactionQueue.addAll(transactions);
                        logger.debug("BlockReader - Block number: " + blockNumber + ", Queue size: " + transactionQueue.size() + ", Thread name: " + Thread.currentThread().getName());
                    } catch (IllegalStateException e) {
                        logger.error("Error adding transactions to queue: ", e);
                    }
                }
            });
        }

        // Create y DBWriter threads
        ForkJoinPool dbWriterExecutor = new ForkJoinPool(y);

        for (int i = 0; i < y; i++) {
            dbWriterExecutor.submit(() -> {
                long startTime = System.currentTimeMillis();
                int recordsWritten = 0;
                while (true) {
                    List<TransactionJava> batch = new ArrayList<>();
                    try {
                        transactionQueue.drainTo(batch, batchSize);
                        if (!batch.isEmpty()) {
                            logger.debug("DBWriter - Queue size: " + transactionQueue.size() + ", Thread name: " + Thread.currentThread().getName());
                            writeTransactions(conn, batch);
                            recordsWritten += batch.size();
                        }
                        
                        long currentTime = System.currentTimeMillis();
                        if (currentTime - startTime >= 60000) { // 60,000 milliseconds = 1 minute
                            logger.info("DBWriter - Records written in the last minute: " + recordsWritten);
                            logger.info("DBWriter - Current block number: " + currentBlockNumber.get());
                            recordsWritten = 0;
                            startTime = currentTime;
                        }
                    } catch (Exception e) {
                        logger.error("Error in DBWriter thread: ", e);
                    }
                }
            });
        }
    }
}
