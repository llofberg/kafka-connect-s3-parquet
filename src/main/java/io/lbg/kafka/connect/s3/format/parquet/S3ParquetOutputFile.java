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

import io.confluent.connect.s3.storage.S3Storage;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

class S3ParquetOutputFile implements OutputFile {
  private S3Storage storage;
  private String filename;
  private S3ParquetPositionOutputStream outputStream;

  S3ParquetOutputFile(S3Storage storage, String filename) {
    this.storage = storage;
    this.filename = filename;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    this.outputStream = new S3ParquetPositionOutputStream(storage, filename);
    return this.outputStream;
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    this.outputStream = new S3ParquetPositionOutputStream(storage, filename);
    return this.outputStream;
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }
}
