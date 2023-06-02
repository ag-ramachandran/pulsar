/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.kusto;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Pulsar sink for Kusto.
 */
@Slf4j
public class KustoSink extends BatchSink<GenericRecord> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
    private IngestClient ingestClient;
    private IngestionProperties ingestionProperties;

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        KustoSinkConfig kustoSinkConfig = KustoSinkConfig.load(config);
        kustoSinkConfig.validate();
        super.init(kustoSinkConfig.getBatchTimeMs(), kustoSinkConfig.getBatchSize());
        KustoClientBuilder clientBuilder = new KustoClientBuilderImpl();
        this.ingestionProperties = new IngestionProperties(kustoSinkConfig.getDatabase(), kustoSinkConfig.getTable());
        this.ingestionProperties.setIngestionMapping(kustoSinkConfig.getIngestionMapping(),
                IngestionMapping.IngestionMappingKind.JSON);
        this.ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON);
        this.ingestClient = clientBuilder.build(kustoSinkConfig);
    }

    @Override
    protected final String buildIngestJsonRecord(Record<GenericRecord> record) {
        val genericRecord = record.getValue();
        // Better to use this as a JSON as it is more flexible than CSV which will need an additional mapping
        if (genericRecord != null) {
            Map<String, Object> jsonToCreate = genericRecord.getFields().stream().collect(Collectors.toMap(
                    Field::getName,
                    genericRecord::getField));
            try {
                return OBJECT_MAPPER.writeValueAsString(jsonToCreate);
            } catch (JsonProcessingException e) {
                // TODO handle this with logs ?
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    protected void ingest(List<String> records) throws Exception {
        UUID sourceId = UUID.randomUUID();
        byte[] bytes = records.stream().collect(Collectors.joining(System.lineSeparator())).getBytes();
        InputStream inputStream = new ByteArrayInputStream(bytes);
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(inputStream, false, sourceId, CompressionType.gz);
        // IngestionProperties properties = new IngestionProperties();
        this.ingestClient.ingestFromStream(streamSourceInfo, this.ingestionProperties);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (null != this.ingestClient) {
            this.ingestClient.close();
        }
    }
}
