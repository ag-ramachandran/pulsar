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

import java.io.File;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

import org.apache.pulsar.io.core.annotations.FieldDoc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Configuration class for the Kusto2 Sink Connector.
 */
@Data
@Accessors(chain = true)
public class KustoSinkConfig implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The url of the Kusto instance to connect to"
    )
    private String clusterUrl;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = "The authentication token used to authenticate to Kusto"
    )
    private String token;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The Kusto organization to write to"
    )
    private String organization;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The Kusto bucket to write to"
    )
    private String bucket;

    @FieldDoc(
            required = false,
            defaultValue = "ONE",
            help = "The timestamp precision for writing data to Kusto. Possible values [ns, us, ms, s]")
    private String precision = "ns";

    @FieldDoc(
            required = false,
            defaultValue = "NONE",
            help = "The log level for Kusto request and response. Possible values [NONE, BASIC, HEADERS, FULL]")
    private String logLevel = "NONE";

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "Flag to determine if gzip should be enabled")
    private boolean gzipEnable = false;

    @FieldDoc(
            required = false,
            defaultValue = "1000L",
            help = "The Kusto operation time in milliseconds")
    private long batchTimeMs = 1000;

    @FieldDoc(
            required = false,
            defaultValue = "200",
            help = "The batch size of write to Kusto database"
    )
    private int batchSize = 200;

    public static KustoSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), KustoSinkConfig.class);
    }

    public static KustoSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsString(map), KustoSinkConfig.class);
    }

    public void validate() {
        Preconditions.checkNotNull(clusterUrl, "clusterUrl property not set.");
        Preconditions.checkNotNull(token, "token property not set.");
        Preconditions.checkNotNull(organization, "organization property not set.");
        Preconditions.checkNotNull(bucket, "bucket property not set.");

        Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive integer.");
        Preconditions.checkArgument(batchTimeMs > 0, "batchTimeMs must be a positive long.");
    }
}

