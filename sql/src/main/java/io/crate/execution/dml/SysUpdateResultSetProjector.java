/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dml;

import io.crate.common.collections.Lists2;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.RowN;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collector;

public class SysUpdateResultSetProjector implements Projector {

    private final Function<Object, List<Object>> rowWriter;
//    private final Symbol[] returnValues;

    public SysUpdateResultSetProjector(Function<Object, List<Object>> rowWriter) {
        this.rowWriter = rowWriter;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator
            .newInstance(batchIterator,
                         Collector.of(
                             () -> new State(rowWriter),
                             (state, row) -> {

                                 List<Object> results = state.rowWriter.apply(row.get(0));
                                 state.resultRows.add(results);
                             },
                             (state1, state2) -> {
                                 throw new UnsupportedOperationException(
                                     "Combine not supported");
                             },
                             state -> Lists2.map(state.resultRows, x ->  new RowN(x.toArray(new Object[]{}))
                         )));
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }

    private static class State {
        final Function<Object, List<Object>> rowWriter;
        final List<List<Object>> resultRows = new ArrayList<>();

        State(Function<Object, List<Object>> rowWriter) {
            this.rowWriter = rowWriter;
        }
    }
}

