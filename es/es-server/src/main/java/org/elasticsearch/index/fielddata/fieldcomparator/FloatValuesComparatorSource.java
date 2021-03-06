/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.NullValueOrder;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 * Comparator source for float values.
 */
public class FloatValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData indexFieldData;

    public FloatValuesComparatorSource(IndexNumericFieldData indexFieldData, NullValueOrder nullValueOrder, MultiValueMode sortMode) {
        super(nullValueOrder, sortMode);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.FLOAT;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final float dMissingValue = (Float) missingObject(nullValueOrder, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new FieldComparator.FloatComparator(numHits, null, null) {
            @Override
            protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                final SortedNumericDoubleValues values = indexFieldData.load(context).getDoubleValues();
                final NumericDoubleValues selectedValues = FieldData.replaceMissing(sortMode.select(values), dMissingValue);
                return selectedValues.getRawFloatValues();
            }
        };
    }
}
