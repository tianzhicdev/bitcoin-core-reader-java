package org.bitcoin.reader;

import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.script.ScriptPattern;
import org.consensusj.bitcoin.jsonrpc.BitcoinClient;

import com.google.common.io.BaseEncoding;

public class Utils {

        public static Connection getDatabaseConnection(Logger logger) throws SQLException{
        Connection connection = null; // todo: make this a singleton
        try {
            String url = "jdbc:postgresql://localhost:3004/bitcoin";
            // String url = "jdbc:postgresql://marcus-mini.is-very-nice.org:3004/bitcoin";
            String user = "abc";
            String password = "12345";

            connection = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            logger.error("Database connection error: ", e);
            throw new SQLException("Failed to establish database connection.", e);
        }
        return connection;
    }


public static BitcoinClient createBitcoinClient(Logger logger) {
    BitcoinClient btcCore = null;
    try {
        btcCore = new BitcoinClient(
            // new URI("http://marcus-mini.is-very-nice.org:3003"),
            new URI("http://localhost:3003"),
            "bitcoinrpc",
            "12345"
        );
    } catch (Exception e) {
        logger.error("Error creating BitcoinClient: ", e);
    }
    return btcCore;
}
    public static List<Transaction> getTransactions(Connection conn, int blockNumber) throws SQLException {
        List<Transaction> transactions = new ArrayList<>();
        String sql = "SELECT txid, block_number, data, readable_data FROM transactions_java_indexed WHERE block_number = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, blockNumber);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    ByteBuffer dataBuffer = ByteBuffer.wrap(rs.getBytes("data"));
                    Transaction transaction = Transaction.read(dataBuffer);
                    transactions.add(transaction);
                }
            }
        }
        return transactions;
    }


    public static String getOutputAddress(TransactionOutput o, Logger logger) {
        try {
            byte[] pubKeyHash = o.getScriptPubKey().getPubKeyHash(); // this is the address
            return BaseEncoding.base16().encode(pubKeyHash);
        } 
        catch (ScriptException e) {
            logger.debug("Script Exception caught: ", e);
            if(ScriptPattern.isP2PK(o.getScriptPubKey())){
                byte[] publicKey = ScriptPattern.extractKeyFromP2PK(o.getScriptPubKey());
                return BaseEncoding.base16().encode(publicKey);
            }
        } catch (Exception e) {
            logger.error("Error processing transaction output: ", e);
            throw e;
        }
        return null;
    }


    public static Transaction getTransaction(Connection conn, String txid) throws SQLException {
        String sql = "SELECT data FROM transactions_java_indexed WHERE txid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, txid);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    ByteBuffer dataBuffer = ByteBuffer.wrap(rs.getBytes("data"));
                    return Transaction.read(dataBuffer);
                } else {
                    throw new SQLException("Transaction not found for txid: " + txid);
                }
            }
        }
    }


    public static TransactionOutput getTransactionOutput(Connection conn, String txid, int outputIndex) throws SQLException {
        Transaction transaction = getTransaction(conn, txid);
        if (transaction != null) {
            List<TransactionOutput> outputs = transaction.getOutputs();
            if (outputIndex >= 0 && outputIndex < outputs.size()) {
                return outputs.get(outputIndex);
            }
        }
        throw new SQLException("Transaction output not found for txid: " + txid + " and outputIndex: " + outputIndex);
    }

    

    public static List<BalanceRecord> getBalanceRecords(Connection conn, Transaction transaction, int blockNumber, Logger logger) throws SQLException {
    List<BalanceRecord> balanceRecords = new ArrayList<>();
    List<TransactionInput> inputs = transaction.getInputs();
    List<TransactionOutput> outputs = transaction.getOutputs();
    
    // for inputs, we can detect if it is coinbase

    for (TransactionInput i: inputs){
        if(i.isCoinBase()){
            // no op
        }else{
            i.getScriptSig();
            String inputTransactionId = i.getOutpoint().hash().toString();
            long inputTransactionIndex = i.getOutpoint().index();
            // Transaction prevTransaction = getTransaction(conn, inputTransactionId);
            TransactionOutput prevOutput = getTransactionOutput(conn, inputTransactionId, (int) inputTransactionIndex);
            String hashAddr = getOutputAddress(prevOutput, logger);
            balanceRecords.add(new BalanceRecord(hashAddr, transaction.getTxId().toString(), blockNumber, -prevOutput.getValue().getValue()));
        }
    }
    for (TransactionOutput o: outputs){
        String hashAddr = getOutputAddress(o, logger);
        // todo: if null, add to unprocessed_transactions
        balanceRecords.add(new BalanceRecord(hashAddr, transaction.getTxId().toString(), blockNumber, o.getValue().getValue()));
    }
        return balanceRecords;
    }
    
}
