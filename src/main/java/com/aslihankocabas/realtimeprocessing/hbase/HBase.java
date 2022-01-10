package com.aslihankocabas.realtimeprocessing.hbase;

import com.aslihankocabas.realtimeprocessing.model.Quote;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBase {
    private final Logger logger = LoggerFactory.getLogger(HBase.class);
    private static final String TABLE_NAME = "quotes";
    private static final String CF_STOCK = "stock";

    Configuration configuration;

    public HBase() {
        configuration = HBaseConfiguration.create();
    }

    public void saveHBase(String data) {
        try (Connection connection = ConnectionFactory.createConnection(configuration); Admin admin = connection.getAdmin()) {

            HTable quotesTable = new HTable(configuration, TABLE_NAME);
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                createQuotesTable(admin);
            }
            for (String quoteJson : data.split("\n")) {
                Quote quote = getQuoteData(quoteJson);
                Put row = new Put(Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
                row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("symbol"), Bytes.toBytes(quote.getSymbol()));
                row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("current_price"), Bytes.toBytes(quote.getC()));
                row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("open_price"), Bytes.toBytes(quote.getO()));
                row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("high_price"), Bytes.toBytes(quote.getH()));
                row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("low_price"), Bytes.toBytes(quote.getL()));
                row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("previous_close"), Bytes.toBytes(quote.getPc()));
                row.add(Bytes.toBytes(CF_STOCK), Bytes.toBytes("time"), Bytes.toBytes(quote.getT()));
                quotesTable.put(row);
            }
            quotesTable.close();

        } catch (IOException e) {
            logger.error("Error occurred while saving data", e);
        }
    }

    private void createQuotesTable(Admin admin) throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        table.addFamily(new HColumnDescriptor(CF_STOCK).setCompressionType(Compression.Algorithm.NONE));
        admin.createTable(table);
    }

    private Quote getQuoteData(String json) {
        Quote quote = null;
        try {
            quote = new ObjectMapper().readValue(json, Quote.class);
        } catch (Exception e) {
            logger.error("Error fetching records from database", e);
        }
        return quote;
    }
}
