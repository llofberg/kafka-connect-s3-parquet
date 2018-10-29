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

import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import org.apache.parquet.io.PositionOutputStream;

import javax.annotation.Nonnull;
import java.io.IOException;

class S3ParquetPositionOutputStream extends PositionOutputStream {
  private final S3OutputStream output;
  private long position = 0;

  S3ParquetPositionOutputStream(S3Storage storage, String filename) {
    output = storage.create(filename, true);
  }

  @Override
  public void write(int b) throws IOException {
    output.write(b);
    position++;
  }

  @Override
  public void write(@Nonnull byte[] b) throws IOException {
    output.write(b);
    position += b.length;
  }

  @Override
  public void write(@Nonnull byte[] b, int off, int len) throws IOException {
    output.write(b, off, len);
    position += len;
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() throws IOException {
    output.commit();
    output.close();
  }

  @Override
  public long getPos() {
    return position;
  }
}
