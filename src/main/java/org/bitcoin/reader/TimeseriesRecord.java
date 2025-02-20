package org.bitcoin.reader;

    // Start of Selection
    public class TimeseriesRecord {
        private java.sql.Timestamp timestamp;
        private int blockNumber;
        private float value;
    
        public TimeseriesRecord() {
        }
    
        public TimeseriesRecord(java.sql.Timestamp timestamp, int blockNumber, float value) {
            this.timestamp = timestamp;
            this.blockNumber = blockNumber;
            this.value = value;
        }
    
        public java.sql.Timestamp getTimestamp() {
            return timestamp;
        }
    
        public void setTimestamp(java.sql.Timestamp timestamp) {
            this.timestamp = timestamp;
        }
    
        public int getBlockNumber() {
            return blockNumber;
        }
    
        public void setBlockNumber(int blockNumber) {
            this.blockNumber = blockNumber;
        }
    
        public float getValue() {
            return value;
        }
    
        public void setValue(float value) {
            this.value = value;
        }
    
        @Override
        public String toString() {
            return "TimeseriesRecord{" +
                    "timestamp=" + timestamp +
                    ", blockNumber=" + blockNumber +
                    ", value=" + value +
                    '}';
        }
    }
