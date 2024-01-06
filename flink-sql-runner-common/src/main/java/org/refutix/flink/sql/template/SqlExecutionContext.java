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

package org.refutix.flink.sql.template;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.operations.BeginStatementSetOperation;
import org.apache.flink.table.operations.EndStatementSetOperation;
import org.apache.flink.table.operations.ExplainOperation;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlExecutionContext {

    private static final Logger log = LoggerFactory.getLogger(SqlExecutionContext.class);
    private TableEnvironment tEnv;

    private List<ModifyOperation> statementOptions;
    private boolean insertSet;

    public SqlExecutionContext(TableEnvironment tEnv) {
        this.tEnv = tEnv;
    }

    public void callOperation(Operation operation) {
        if (operation instanceof SetOperation) {
            callSet((SetOperation) operation);
        } else if (operation instanceof BeginStatementSetOperation) {
            callBeginStatementSet();
        } else if (operation instanceof EndStatementSetOperation) {
            callEndStatementSet();
        } else if (operation instanceof SinkModifyOperation) {
            callInsert((SinkModifyOperation) operation);
        } else if (operation instanceof ExplainOperation) {
            callExplain((ExplainOperation) operation);
        } else {
            executeOperation(operation);
        }
    }

    private void callSet(SetOperation setOperation) {
        if (setOperation.getKey().isPresent() && setOperation.getValue().isPresent()) {
            String key = setOperation.getKey().get().trim();
            String value = setOperation.getValue().get().trim();
            tEnv.getConfig().set(key, value);
        } else {
            log.error("key or value is null");
        }
    }

    private void callInsert(SinkModifyOperation operation) {
        if (insertSet) {
            statementOptions.add(operation);
            log.info("add statement to set");
        } else {
            callInserts(Collections.singletonList(operation));
        }
    }

    private void callInserts(List<ModifyOperation> operationList) {
        ((TableEnvironmentInternal) tEnv).executeInternal(operationList);
    }

    private void callExplain(ExplainOperation operation) {
        ((TableEnvironmentInternal) tEnv).executeInternal(operation);
    }

    private void callBeginStatementSet() {
        insertSet = true;
        statementOptions = new ArrayList<>();
    }

    private void callEndStatementSet() {
        if (insertSet) {
            callInserts(statementOptions);
        }
    }

    private void callAddJar() {
        //TODO
    }

    private void executeOperation(Operation operation){
        ((TableEnvironmentInternal) tEnv).executeInternal(operation);
    }
}
