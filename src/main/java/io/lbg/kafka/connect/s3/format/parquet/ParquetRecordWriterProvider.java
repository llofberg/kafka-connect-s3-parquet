/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Modifications by @lny 2018


package io.lbg.kafka.connect.s3.format.parquet;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ParquetRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private final S3Storage storage;
  private final AvroData avroData;
  private S3ParquetOutputFile outputFile;

  ParquetRecordWriterProvider(S3Storage storage, AvroData avroData) {
    this.storage = storage;
    this.avroData = avroData;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    // This is not meant to be a thread-safe writer!
    return new RecordWriter() {
      ParquetWriter<GenericData.Record> writer = null;
      Schema schema = null;

      @Override
      public void write(SinkRecord record) {
        if (schema == null || writer == null) {
          schema = record.valueSchema();
          try {
            log.info("Opening record writer for: {}", filename);
            outputFile = new S3ParquetOutputFile(storage, filename);
            writer = AvroParquetWriter
              .<GenericData.Record>builder(outputFile)
              .withRowGroupSize(256 * 1024 * 1024)
              .withPageSize(128 * 1024)
              .withSchema(avroData.fromConnectSchema(schema))
              .withConf(new Configuration())
              .withCompressionCodec(CompressionCodecName.SNAPPY)
              .withValidation(true)
              .withDictionaryEncoding(false)
              .build();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        log.trace("Sink record: {} - {}", record, record.getClass().getName());
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          if (value instanceof GenericData.Record) {
            writer.write((GenericData.Record) value);
          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        fin();
      }

      @Override
      public void close() {
        fin();
      }

      private void fin() {
        try {
          writer.close();
          writer = null;
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }


}
