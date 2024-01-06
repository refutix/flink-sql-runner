/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.refutix.flink.sql.template.common;

import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.Operation;
import org.refutix.flink.sql.template.exceptions.SqlParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlParseUtil {

    private static final String STATEMENT_DELIMITER = ";";

    private static final String MASK = "--.*$";

    private static final String BEGINNING_MASK = "^(\\s)*--.*$";

    private SqlParseUtil() {

    }

    public static Operation parseSingleStatement(Parser parser, String statement) {
        List<Operation> operations;
        try {
            operations = parser.parse(statement);
        } catch (Exception e) {
            throw new SqlParseException("Failed to parse sql statement: " + statement, e);
        }
        if (operations.isEmpty()) {
            throw new SqlParseException("Failed to parse sql statement: " + statement);
        }
        return operations.get(0);
    }

    public static String formatSql(String sql) {
        if (sql == null) {
            return "";
        }
        String trimmed = sql.trim();
        StringBuilder formatted = new StringBuilder();
        formatted.append(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(";");
        }
        return formatted.toString();
    }

    public static List<String> splitContent(String content) {
        List<String> statements = new ArrayList<>();
        List<String> buffer = new ArrayList<>();
        for (String line : content.split("\n")) {
            if (isEndOfStatement(line)) {
                buffer.add(line);
                statements.add(normalize(buffer));
                buffer.clear();
            } else {
                buffer.add(line);
            }

        }
        return statements;
    }

    private static String normalize(List<String> buffer) {
        return buffer.stream().map(line -> line.replaceAll(BEGINNING_MASK, ""))
                .collect(Collectors.joining("\n"));
    }

    private static boolean isEndOfStatement(String line) {
        return line.replaceAll(MASK, "").trim().endsWith(";");
    }
}
