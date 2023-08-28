/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.sharing.examples;

import scala.Option;
import scala.collection.Iterator;

import io.delta.sharing.client.DeltaSharingClient;
import io.delta.sharing.client.DeltaSharingFileProfileProvider;
import io.delta.sharing.client.DeltaSharingRestClient;
import io.delta.sharing.client.model.DeltaTableFiles;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.Scan;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.DataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

/**
 * Single threaded Delta sharing table reader using the Delta Kernel APIs.
 *
 * <p>
 * Usage: java io.delta.sharing.examples.KernelBasedJavaSharingClient [-c <arg>] [-l <arg>] -t <arg>
 * <p>
 * -c,--columns <arg>   Comma separated list of columns to read from the
 * table. Ex. --columns=id,name,address
 * -l,--limit <arg>     Maximum number of rows to read from the table
 * (default 20).
 * -t,--table <arg>     Fully qualified table path
 * </p>
 */
public class KernelBasedJavaSharingClient
    extends BaseTableReader
{
    public void show() throws IOException
    {
        Configuration hadoopConf = new Configuration()
        {{
            set("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider");
            set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com");
        }};
        TableClient tableClient = DefaultTableClient.create(hadoopConf);

        DeltaSharingFileProfileProvider shareProvider = new DeltaSharingFileProfileProvider(
            hadoopConf,
            // this.getClass().getResource("test-server-profile").toString()
            "/Users/venkateshwar.korukanti/opensource/delta-sharing/examples/java/" +
                "kernel-based-java-client/src/main/resources/test-server-profile"
        );
        DeltaSharingClient sharingClient = new DeltaSharingRestClient(
            shareProvider,
            120,
            10,
            Long.MAX_VALUE,
            false,
            false,
            "kernel",
            "",
            false,
            100000
        );

        DeltaTableFiles files = sharingClient.getFiles(
            new io.delta.sharing.client.model.Table("hackathon_dv_table", "default", "share1"),
            null,
            Option.empty(),
            Option.empty(),
            Option.empty(),
            Option.empty()
        );

        String scanStateStr = files.kernelStateAndScanFiles().head();
        List<String> scanFilesStr = scala.collection.JavaConverters.seqAsJavaListConverter(
            files.kernelStateAndScanFiles().drop(1).toSeq()).asJava();

        Row scanState = KernelUtilities.deserializeRowFromJson(tableClient, scanStateStr);
        List<Row> scanFilesIter = scanFilesStr.stream().map(
            scanFileStr -> KernelUtilities.deserializeRowFromJson(tableClient, scanFileStr)
        ).collect(Collectors.toList());

        boolean printedSchema = false;
        try (CloseableIterator<DataReadResult> data =
            Scan.readData(
                tableClient,
                scanState,
                Utils.toCloseableIterator(scanFilesIter.iterator()),
                Optional.empty())) {
            while (data.hasNext()) {
                DataReadResult dataReadResult = data.next();
                if (!printedSchema) {
                    printSchema(dataReadResult.getData().getSchema());
                    printedSchema = true;
                }
                printData(dataReadResult, Integer.MAX_VALUE);
            }
        }
    }

    public static void main(String[] args)
        throws Exception
    {
        new KernelBasedJavaSharingClient().show();
    }
}
