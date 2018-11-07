package com.vaishali.kafka.connect;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.*;


public class JenkinsBuilder {
    private static final Logger log = LoggerFactory.getLogger(JenkinsBuilder.class);
    JenkinsSinkConnectorConfig config;
    List<String> domoRecords;
    private Map<TopicPartition, Long> lastCommitOffsets;
    private Map<TopicPartition, Long> currentRecordOffsets;
    private SinkTaskContext context;
    private boolean committing;

    com.domo.sdk.DomoClient oldDC;

    String dataSetId;
    long partNum = 0;
    int maxRetries = 5;
    Date lastCommitTime;

    Collection<Collection<Object>> csvPrinter;
    StringWriter csvContents;

    int recordCount = 0;
    int commitInterval;

    public JenkinsBuilder() {

    }

    // For testing purposes
    public JenkinsBuilder(JenkinsSinkConnectorConfig config) {
        this.config = config;
        domoRecords = new ArrayList<>();
        lastCommitOffsets = new HashMap<>();
        currentRecordOffsets = new HashMap<>();
    }

    public JenkinsBuilder(JenkinsSinkConnectorConfig config, SinkTaskContext context) {
        this.config = config;
        this.context = context;
        this.dataSetId = dataSetId;
        domoRecords = new ArrayList<>();
        lastCommitOffsets = new HashMap<>();
        currentRecordOffsets = new HashMap<>();
        lastCommitTime = new Date();
        csvPrinter = new ArrayList<>();

        committing = false;
        recordCount = 0;
        commitInterval = config.getCommitInterval();

    }

    public void write (Collection<SinkRecord> records) {

        //Transform into escaped csv records
        log.trace("In write for records: {}", records.size());
        for(SinkRecord record: records) {
            //get or create dataSetId
            updateKafkaOffsets(record);
            appendRecord(record);
        }

        //Upload chunk
        if (recordCount > config.getBatchSize()) {
            uploadChunk();
        }

        // Finalize commit if > commit.interval mins elapsed?
        Date now = new Date();
        if (dateDiffInMinutes(lastCommitTime, now) > commitInterval) {
            // flush remaining records
            if (recordCount > 0) {
                uploadChunk();
            }
            commit(now);
        }

    }

    private void appendRecord(SinkRecord record) {
        ArrayList<Object> strRecord = new ArrayList<>();
        Struct curRecord = (Struct) record.value();
        for (Field f : record.valueSchema().fields()) {
            if (curRecord.get(f) != null) {
                if (f.schema().name() != null
                        && f.schema().name().equalsIgnoreCase("org.apache.kafka.connect.data.Timestamp")
                        && f.schema().type() == Schema.Type.INT64) {
                    // added to accommodate the funky connect timestamp logical type
                    strRecord.add(f.index(), String.valueOf(((Date)curRecord.get(f)).getTime()));
                } else {
                    strRecord.add(f.index(), curRecord.get(f).toString());
                }
            } else {
                strRecord.add(f.index(), "");
            }
        }

        csvPrinter.add(strRecord);
        recordCount++;
    }

    private void updateKafkaOffsets(SinkRecord record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        currentRecordOffsets.put(tp, record.kafkaOffset());

        if (lastCommitOffsets.get(tp) == null) {
            if (record.kafkaOffset() == 0) {
                lastCommitOffsets.put(tp, record.kafkaOffset());
            } else {
                lastCommitOffsets.put(tp, record.kafkaOffset() - 1);
            }
        }
    }

    private void uploadChunk () {

        //Create Execution if not exists
        if (null == null ) {
            log.info("Initiated Execution: {} for data set ID: {}.", dataSetId);
        }

        int count = 0;
        while(true) {
            try {
                Thread.sleep(count * 1000);
                log.info("Uploading part: {}, for execution: {} stream: {}. Attempt : {}",
                        partNum, dataSetId, (count + 1));

                break;
            } catch (InterruptedException e) {
                throw new ConnectException(e);
            }
        }
        partNum+=1;
        recordCount = 0;
        csvPrinter = new ArrayList<>();

    }

    private void commit (Date now) {

//        if (initResponse != null) {
//            int count = 0;
//            while (true) {
//                try {
//                    Thread.sleep(count * 1000);
//                    log.info("Attempting Execution commit, attempt num: {}", (count + 1));
//                    manager.indexData(initResponse, dataSetId, true);
//                    log.info("Execution commit successful, for data set ID: {}", dataSetId);
//                    break;
//                } catch (RequestFailedException e) {
//                    if (++count == maxRetries) throw new RetriableException(e);
//                } catch (InterruptedException e) {
//                    throw new ConnectException(e);
//                }
//            }
//
//            initResponse = null;
//            partNum = 0;
//        }

        lastCommitTime = now;
        requestOffsetCommit();
    }

    private void requestOffsetCommit() {
        log.trace("Setting commit offsets");
        for (TopicPartition tp: currentRecordOffsets.keySet()) {
            log.trace("Topic-Parition {} at offset {}", tp, currentRecordOffsets.get(tp));
            lastCommitOffsets.put(tp, currentRecordOffsets.get(tp));
        }

        //request kafka to commit offsets
        log.trace("Requesting offset commit.");
        context.requestCommit();
        committing = true;
    }

    private long dateDiffInMinutes (Date d1, Date d2) {
        return (d2.getTime() - d1.getTime()) / 60000;
    }


    public void stop () {
        // abort active Execution
//        if (initResponse != null) {
//            //TODO abort execution
//            log.info("Aborting Execution for data set Id: {}", dataSetId);
//        }
    }

    public Map<TopicPartition, Long> getOffsetsToCommitAndReset () {
        if (committing) {
            Map<TopicPartition, Long> offsetCopy = new HashMap<>(lastCommitOffsets);
            lastCommitOffsets.clear();
            committing = false;
            return offsetCopy;
        }
        else {
            return null;
        }
    }

    // Visible for testing
    public com.domo.sdk.datasets.model.Schema inferSchema (Schema schema) {
//        com.domo.sdk.datasets.model.Schema domoSchema = new com.domo.sdk.datasets.model.Schema();
//        Column[] cols = new Column[schema.fields().size()];
//
//        for (Field f: schema.fields()) {
//            switch(f.schema().type()) {
//                case INT8:
//                case INT16:
//                case INT32:
//                case INT64:
//                    cols[f.index()] = new Column(ColumnType.LONG, f.name());
//                    break;
//                case FLOAT32:
//                case FLOAT64:
//                    cols[f.index()] = new Column(ColumnType.DOUBLE, f.name());
//                    break;
//                case BOOLEAN:
//                case ARRAY:
//                case STRING:
//                    cols[f.index()] = new Column(ColumnType.STRING, f.name());
//                    break;
//                default:
//                    throw new ConnectException("Unsupported source data type: "+ f.schema().type());
//            }
//        }
//
//        domoSchema.setColumns(new ArrayList<>(Arrays.asList(cols)));
//        return domoSchema;
        return null;

    }

}
