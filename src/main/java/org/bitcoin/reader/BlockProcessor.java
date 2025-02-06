package org.bitcoin.reader;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.consensusj.bitcoin.jsonrpc.BitcoinClient;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;

public class BlockProcessor extends AbstractRWProcessor<TransactionJava> {
    
    private static final Logger logger = org.apache.logging.log4j.LogManager.getLogger(BlockProcessor.class);
    private final BitcoinClient btcCore;

    public BlockProcessor() throws SQLException {
        super(logger, "transactions_java_indexed");
        this.btcCore = Utils.createBitcoinClient(logger); // Assuming Utils has a method to create BitcoinClient
    }

    @Override
    protected List<TransactionJava> read(int blockNumber) throws Exception {
        List<TransactionJava> transactions = new ArrayList<>();
        try {
            Sha256Hash blockHash = btcCore.getBlockHash(blockNumber);
            BlockProcessor block = btcCore.getBlock(blockHash);

            for (Transaction tx : block.getTransactions()) {
                TransactionJava transactionJava = new TransactionJava(tx.getTxId().toString(), blockNumber, tx.serialize(), tx.toString());
                transactions.add(transactionJava);
            }
        } catch (Exception e) {
            logger.error("Error getting transactions for block " + blockNumber + ": ", e);
            throw e;
        }
        return transactions;
    }

    @Override
    protected void write(List<TransactionJava> transactions) throws SQLException {
        long startTime = System.currentTimeMillis(); // Start metering
        refreshDatabaseConnection(); // Corrected method call
        if (conn == null || transactions == null || transactions.isEmpty()) {
            throw new SQLException("Connection is null or transactions list is empty.");
        }

        String sql = "INSERT INTO transactions_java_indexed (txid, block_number, data, readable_data) VALUES (?, ?, ?, ?) ON CONFLICT (txid, block_number) DO NOTHING";

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

    public static void main(String[] args) throws Exception {
        int x = args.length > 0 ? Integer.parseInt(args[0]) : 1; // Number of BlockReader threads
        int y = args.length > 1 ? Integer.parseInt(args[1]) : 1;  // Number of DBWriter threads
        int queueSize = args.length > 2 ? Integer.parseInt(args[2]) : 100; // Queue size for transactionQueue
        int smallestSize = args.length > 3 ? Integer.parseInt(args[3]) : 20; // Smallest size for DBWriter
        int maxBatchSize = args.length > 4 ? Integer.parseInt(args[4]) : 10; // Max batch size for DBWriter

        BlockProcessor loader = new BlockProcessor();

        loader.execute(x, y, queueSize, smallestSize, maxBatchSize);
    }
}
