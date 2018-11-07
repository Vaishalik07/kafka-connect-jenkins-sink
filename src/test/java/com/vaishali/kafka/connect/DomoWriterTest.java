package com.vaishali.kafka.connect;

import com.domo.sdk.datasets.model.Column;
import com.domo.sdk.datasets.model.ColumnType;
import com.domo.sdk.streams.StreamClient;
import com.domo.sdk.streams.model.Stream;
import com.domo.sdk.streams.model.StreamRequest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Arrays;

import static org.mockito.Mockito.*;

/**
 * Created by vikas.tikoo on 5/8/17.
 */
public class DomoWriterTest {

}
