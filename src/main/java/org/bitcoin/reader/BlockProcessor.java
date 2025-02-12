package org.bitcoin.reader;

import org.bitcoinj.base.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.consensusj.bitcoin.jsonrpc.BitcoinClient;

import java.sql.Connection;
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
    protected List<TransactionJava> read(Connection conn, int fromBlockNumber, int toBlockNumber) throws Exception {
        List<TransactionJava> transactions = new ArrayList<>();
        for (int blockNumber = fromBlockNumber; blockNumber < toBlockNumber; blockNumber++) {
            try {
                Sha256Hash blockHash = btcCore.getBlockHash(blockNumber);
                Block block = btcCore.getBlock(blockHash);

                for (Transaction tx : block.getTransactions()) {
                    TransactionJava transactionJava = new TransactionJava(tx.getTxId().toString(), blockNumber, tx.serialize(), tx.toString());
                    transactions.add(transactionJava);
                }
            } catch (Exception e) {
                logger.error("Error getting transactions for block " + blockNumber + ": ", e);
                throw e;
            }
        }
        return transactions;
    }

    @Override
    protected void write(Connection conn, List<TransactionJava> transactions) throws SQLException {

        String sql = "INSERT INTO transactions_java_indexed " +
                    "(txid, block_number, data, readable_data) " +
                    "VALUES (?, ?, ?, ?) " +
                    "ON CONFLICT (txid, block_number) DO NOTHING";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (TransactionJava transaction : transactions) {
                pstmt.setString(1, transaction.getTxid());
                pstmt.setInt(2, transaction.getBlockNumber());
                byte[] data = transaction.getData();
                writtenBytesCounter.addAndGet(data.length);
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
        int x = args.length > 0 ? Integer.parseInt(args[0]) : 1; // Number of BlockReader threads
        int y = args.length > 1 ? Integer.parseInt(args[1]) : 1;  // Number of DBWriter threads
        int queueSize = args.length > 2 ? Integer.parseInt(args[2]) : 100; // Queue size for transactionQueue
        int readBatchSize = args.length > 3 ? Integer.parseInt(args[3]) : 5; // Read batch size for BlockReader
        int smallestSize = args.length > 4 ? Integer.parseInt(args[4]) : 20; // Smallest size for DBWriter
        int maxBatchSize = args.length > 5 ? Integer.parseInt(args[5]) : 10; // Max batch size for DBWriter

        BlockProcessor loader = new BlockProcessor();

        loader.execute(x, y, queueSize, readBatchSize, smallestSize, maxBatchSize);
    }
}
