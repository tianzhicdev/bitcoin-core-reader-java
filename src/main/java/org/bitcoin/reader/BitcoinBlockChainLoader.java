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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
            e.printStackTrace();
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
            e.printStackTrace();
        }

        return highestBlock;
    }

    static public Boolean writeTransactions(Connection conn, List<TransactionJava> transactions) {
        // INSERT_YOUR_CODE
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
            e.printStackTrace();
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
            e.printStackTrace();
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

        // Create x BlockReader threads
        ExecutorService blockReaderExecutor = Executors.newFixedThreadPool(x);

        for (int i = 0; i < x; i++) {
            blockReaderExecutor.submit(() -> {
                while (true) {
                    int blockNumber = currentBlockNumber.getAndIncrement();
                    List<TransactionJava> transactions = getTransactions(btcCore, blockNumber);
                    try {
                        transactionQueue.addAll(transactions);
                        logger.info("BlockReader - Block number: " + blockNumber + ", Queue size: " + transactionQueue.size() + ", Thread name: " + Thread.currentThread().getName());
                    } catch (IllegalStateException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        // Create y DBWriter threads
        ExecutorService dbWriterExecutor = Executors.newFixedThreadPool(y);

        for (int i = 0; i < y; i++) {
            dbWriterExecutor.submit(() -> {
                while (true) {
                    List<TransactionJava> batch = new ArrayList<>();
                    try {
                        transactionQueue.drainTo(batch, batchSize);
                        if (!batch.isEmpty()) {
                            logger.info("DBWriter - Queue size: " + transactionQueue.size() + ", Thread name: " + Thread.currentThread().getName());
                            writeTransactions(conn, batch);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
