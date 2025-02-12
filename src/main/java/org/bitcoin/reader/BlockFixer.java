package org.bitcoin.reader;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.consensusj.bitcoin.jsonrpc.BitcoinClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;

public class BlockFixer extends AbstractRWProcessor<TransactionJava> {
    
    private static final Logger logger = org.apache.logging.log4j.LogManager.getLogger(BlockFixer.class);
    private final BitcoinClient btcCore;

    public BlockFixer() throws SQLException {
        super(logger, "transactions_java_indexed");
        this.btcCore = Utils.createBitcoinClient(logger); // Assuming Utils has a method to create BitcoinClient
    }

    @Override
    protected int getHighestBlockNumber(Connection conn, String tableName) throws SQLException {
        return 400000;
    }

    @Override
    protected List<TransactionJava> read(Connection conn, int fromBlockNumber, int toBlockNumber) throws Exception {
        List<TransactionJava> transactions = new ArrayList<>();
        List<String> existingTxids = fetchExistingTxids(conn, fromBlockNumber, toBlockNumber);

        boolean allBlocksValid = true;
        for (int blockNumber = fromBlockNumber; blockNumber < toBlockNumber; blockNumber++) {
            try {
                Sha256Hash blockHash = btcCore.getBlockHash(blockNumber);
                Block block = btcCore.getBlock(blockHash);

                boolean missingTransaction = false;
                for (Transaction tx : block.getTransactions()) {
                    String txid = tx.getTxId().toString();
                    if (!existingTxids.contains(txid)) {
                        logger.info("Missing transaction detected: txid = " + txid + ", block number = " + blockNumber);
                        TransactionJava transactionJava = new TransactionJava(txid, blockNumber, tx.serialize(), tx.toString());
                        transactions.add(transactionJava);
                        missingTransaction = true;
                    }
                }
                if (missingTransaction) {
                    allBlocksValid = false;
                }
            } catch (Exception e) {
                logger.error("Error getting transactions for block " + blockNumber + ": ", e);
                throw e;
            }
        }
        if (allBlocksValid) {
            logger.info("All blocks are valid from block number " + fromBlockNumber + " to block number " + toBlockNumber);
        }
        return transactions;
    }

    private List<String> fetchExistingTxids(Connection conn, int fromBlockNumber, int toBlockNumber) throws SQLException {
        List<String> txids = new ArrayList<>();
        String sql = "SELECT txid FROM transactions_java_indexed WHERE block_number >= ? AND block_number < ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, fromBlockNumber);
            pstmt.setInt(2, toBlockNumber);
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                txids.add(resultSet.getString("txid"));
            }
        }
        return txids;
    }

    @Override
    protected void write(Connection conn, List<TransactionJava> transactions) throws SQLException {

        String sql = "INSERT INTO transactions_java_indexed (txid, block_number, data, readable_data) VALUES (?, ?, ?, ?) ON CONFLICT (txid, block_number) DO NOTHING";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (TransactionJava transaction : transactions) {
                pstmt.setString(1, transaction.getTxid());
                pstmt.setInt(2, transaction.getBlockNumber());
                byte[] data = transaction.getData();
                pstmt.setBytes(3, data);
                pstmt.setString(4, transaction.getReadableData());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
       } catch (SQLException e) {
            logger.error("Error writing transactions: ", e);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        int readerThreads = args.length > 0 ? Integer.parseInt(args[0]) : 1; // Number of BlockReader threads
        int writerThreads = args.length > 1 ? Integer.parseInt(args[1]) : 1;  // Number of DBWriter threads
        int queueSize = args.length > 2 ? Integer.parseInt(args[2]) : 100; // Queue size for transactionQueue
        int readBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : 5; // Read batch size for BlockReader
        int minBatchSize = args.length > 4 ? Integer.parseInt(args[4]) : 20; // Smallest size for DBWriter
        int maxBatchSize = args.length > 5 ? Integer.parseInt(args[5]) : 10; // Max batch size for DBWriter

        BlockFixer loader = new BlockFixer();

        loader.execute(readerThreads, writerThreads, queueSize, readBatchSize, minBatchSize, maxBatchSize);
    }
}
