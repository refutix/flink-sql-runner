/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.refutix.flink.sql.template;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.refutix.flink.sql.template.common.SqlParseUtil;
import org.refutix.flink.sql.template.provider.sql.FlinkSqlProvider;
import org.refutix.flink.sql.template.provider.sql.FlinkSqlProviderFactory;

import java.time.ZoneId;
import java.util.List;
import java.util.Properties;

/**
 * Main class for executing SQL scripts.
 */
public class SqlRunner {

    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Properties properties = parameterTool.getProperties();
        FlinkSqlProvider provider = FlinkSqlProviderFactory.getProvider(properties);
        String sql = provider.getSql(properties);
        String executionMode = parameterTool.get("mode", "streaming");
        String formattedSql = SqlParseUtil.formatSql(sql);
        EnvironmentSettings fs;
        if ("streaming".equals(executionMode)) {
            fs = EnvironmentSettings.newInstance().inStreamingMode().build();
        } else {
            fs = EnvironmentSettings.newInstance().inBatchMode().build();
        }

        TableEnvironment tEnv = TableEnvironment.create(fs);
        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        SqlExecutionContext sqlExecutionContext = new SqlExecutionContext(tEnv);
        Parser parser = ((TableEnvironmentInternal) tEnv).getParser();

        List<String> sqls = SqlParseUtil.splitContent(formattedSql);
        for (String s : sqls) {
            s = s.trim();
            if(s.isEmpty() || ";".equals(s)){
                continue;
            }
            if(s.endsWith(";")){
                s = s.substring(0,s.length()-1).trim();
            }
            Operation operation = SqlParseUtil.parseSingleStatement(parser, s);
            sqlExecutionContext.callOperation(operation);
        }
    }
}
