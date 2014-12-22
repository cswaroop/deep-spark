/*
 * Copyright 2014, Stratio.
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
package com.stratio.deep.aerospike.extractor;

import com.aerospike.client.Record;
import com.stratio.deep.aerospike.reader.AerospikeReader;
import com.stratio.deep.aerospike.writer.AerospikeWriter;
import com.stratio.deep.core.entity.MessageTestEntity;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.api.mockito.PowerMockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * AerospikeNativeEntityExtractor unit tests.
 */
@Test(groups = {"UnitTests"})
public class AerospikeNativeEntityExtractorTest {

    private AerospikeReader reader = PowerMockito.mock(AerospikeReader.class);

    private AerospikeWriter writer = PowerMockito.mock(AerospikeWriter.class);

    @Test
    public void testSaveRdd() {
        AerospikeNativeEntityExtractor extractor = createAerospikeNativeExtractor();
        extractor.saveRDD(new MessageTestEntity());
        verify(writer, times(1)).save(any(Record.class));
    }

    private AerospikeNativeEntityExtractor createAerospikeNativeExtractor() {
        AerospikeNativeEntityExtractor extractor = new AerospikeNativeEntityExtractor(MessageTestEntity.class);
        Whitebox.setInternalState(extractor, "reader", reader);
        Whitebox.setInternalState(extractor, "writer", writer);
        return extractor;
    }
}
