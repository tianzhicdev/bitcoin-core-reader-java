package org.bitcoin.reader;

public class TransactionJava {
    private String txid;
    private int blockNumber;
    private byte[] data;
    private String readableData;

    public TransactionJava(String txid, int blockNumber, byte[] data, String readableData) {
        this.txid = txid;
        this.blockNumber = blockNumber;
        this.data = data;
        this.readableData = readableData;
    }

    public String getTxid() {
        return txid;
    }

    public void setTxid(String txid) {
        this.txid = txid;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public void setBlockNumber(int blockNumber) {
        this.blockNumber = blockNumber;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getReadableData() {
        return readableData;
    }

    public void setReadableData(String readableData) {
        this.readableData = readableData;
    }
}
