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

package com.stratio.deep.cassandra.extractor;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.deep.cassandra.config.CassandraConfigFactory;
import com.stratio.deep.cassandra.context.AbstractDeepSparkContextTest;
import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.cassandra.embedded.CassandraServer;
import com.stratio.deep.commons.exception.DeepIOException;
import com.stratio.deep.commons.exception.DeepIndexNotFoundException;
import com.stratio.deep.commons.exception.DeepNoSuchFieldException;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.commons.functions.AbstractSerializableFunction;
import com.stratio.deep.cassandra.testentity.Cql3TestEntity;
import com.stratio.deep.commons.utils.Constants;
import org.apache.spark.rdd.RDD;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Function1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

/**
 * Created by luca on 03/02/14.
 */
@Test(suiteName = "cassandraRddTests", groups = {"CassandraCql3RDDTest"}, dependsOnGroups = {"CassandraEntityRDDTest"})
public class CassandraCql3RDDTest extends CassandraRDDTest<Cql3TestEntity> {

    private static class TestEntityAbstractSerializableFunction extends
            AbstractSerializableFunction<Cql3TestEntity, Cql3TestEntity> {
        private static final long serialVersionUID = 6678218192781434399L;

        @Override
        public Cql3TestEntity apply(Cql3TestEntity e) {
            return new Cql3TestEntity(e.getName(), e.getPassword(), e.getColor(), e.getGender(), e.getFood(),
                    e.getAnimal(), e.getLucene());
        }
    }

    @Override
    protected void checkComputedData(Cql3TestEntity[] entities) {

        boolean found = false;

        Assert.assertEquals(entities.length, AbstractDeepSparkContextTest.cql3TestDataSize);

        for (Cql3TestEntity e : entities) {
            if (e.getName().equals("pepito_3") && e.getAge().equals(-2) && e.getGender().equals("male")
                    && e.getAnimal().equals("monkey")) {
                assertNull(e.getColor());
                assertNull(e.getLucene());
                Assert.assertEquals(e.getFood(), "donuts");
                Assert.assertEquals(e.getPassword(), "abc");
                found = true;
                break;
            }
        }

        if (!found) {
            fail();
        }

    }

    @Test
    public void testAdditionalFilters() {
        try {
            CassandraConfigFactory
                    .create(Cql3TestEntity.class)
                    .host(Constants.DEFAULT_CASSANDRA_HOST)
                    .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                    .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                    .keyspace(AbstractDeepSparkContextTest.KEYSPACE_NAME)
                    .columnFamily(AbstractDeepSparkContextTest.CQL3_COLUMN_FAMILY)
                    .filterByField("notExistentField", "val")
                    .initialize();

            fail();
        } catch (DeepNoSuchFieldException e) {
            // OK
        }

        try {
            CassandraConfigFactory
                    .create(Cql3TestEntity.class)
                    .host(Constants.DEFAULT_CASSANDRA_HOST)
                    .rpcPort(CassandraServer.CASSANDRA_THRIFT_PORT)
                    .cqlPort(CassandraServer.CASSANDRA_CQL_PORT)
                    .keyspace(AbstractDeepSparkContextTest.KEYSPACE_NAME)
                    .columnFamily(AbstractDeepSparkContextTest.CQL3_COLUMN_FAMILY)
                    .filterByField("lucene", "val")
                    .initialize();

            fail();
        } catch (DeepIndexNotFoundException e) {
            // OK
        }

        Cql3TestEntity[] entities = (Cql3TestEntity[]) rdd.collect();

        int allElements = entities.length;
        assertTrue(allElements > 1);

        ExtractorConfig<Cql3TestEntity> config = new ExtractorConfig<>(Cql3TestEntity.class);

        RDD<Cql3TestEntity> otherRDD = AbstractDeepSparkContextTest.context.createRDD(config);

        entities = (Cql3TestEntity[]) otherRDD.collect();

        assertEquals(entities.length, 1);
        Assert.assertEquals(entities[0].getFood(), "donuts");
        Assert.assertEquals(entities[0].getName(), "pepito_3");
        Assert.assertEquals(entities[0].getGender(), "male");
        Assert.assertEquals(entities[0].getAge(), Integer.valueOf(-2));
        Assert.assertEquals(entities[0].getAnimal(), "monkey");

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST,    Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.KEYSPACE, AbstractDeepSparkContextTest.KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY,  AbstractDeepSparkContextTest.CQL3_COLUMN_FAMILY);
        values.put(ExtractorConstants.CQLPORT,  String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));


        config.setValues(values);



        otherRDD = AbstractDeepSparkContextTest.context.createRDD(config);

        entities = (Cql3TestEntity[]) otherRDD.collect();
        assertEquals(entities.length, allElements - 1);

        /*
        otherRDD.removeFilterOnField("food");
        entities = (Cql3TestEntity[]) otherRDD.collect();
        assertEquals(entities.length, allElements);
        */
    }


    protected void checkOutputTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME + "." + AbstractDeepSparkContextTest.CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

        ResultSet rs = session.execute(command);
        assertEquals(rs.one().getLong(0), 4);

        command = "SELECT * from " + AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME + "." + AbstractDeepSparkContextTest.CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

        rs = session.execute(command);
        for (Row r : rs) {
            assertEquals(r.getInt("age"), 15);
        }
        session.close();
    }

    @Override
    protected void checkSimpleTestData() {
        Cluster cluster = Cluster.builder().withPort(CassandraServer.CASSANDRA_CQL_PORT)
                .addContactPoint(Constants.DEFAULT_CASSANDRA_HOST).build();
        Session session = cluster.connect();

        String command = "select count(*) from " + AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME + "." + AbstractDeepSparkContextTest.CQL3_ENTITY_OUTPUT_COLUMN_FAMILY + ";";

        ResultSet rs = session.execute(command);
        Assert.assertEquals(rs.one().getLong(0), AbstractDeepSparkContextTest.cql3TestDataSize);

        command = "select * from " + AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME + "." + AbstractDeepSparkContextTest.CQL3_ENTITY_OUTPUT_COLUMN_FAMILY
                + " WHERE name = 'pepito_1' and gender = 'male' and age = 0  and animal = 'monkey';";
        rs = session.execute(command);

        List<Row> rows = rs.all();

        assertNotNull(rows);
        assertEquals(rows.size(), 1);

        Row r = rows.get(0);

        assertEquals(r.getString("password"), "xyz");

        session.close();
    }

    @Override
    protected RDD<Cql3TestEntity> initRDD() {
        assertNotNull(AbstractDeepSparkContextTest.context);
        return AbstractDeepSparkContextTest.context.createRDD(getReadConfig());
    }

    @Override
    protected ExtractorConfig<Cql3TestEntity> initReadConfig() {


        ExtractorConfig<Cql3TestEntity> writeConfig = new ExtractorConfig<>(Cql3TestEntity.class);

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST,    Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.KEYSPACE, AbstractDeepSparkContextTest.KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY,  AbstractDeepSparkContextTest.CQL3_COLUMN_FAMILY);
        values.put(ExtractorConstants.CQLPORT,  String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.PAGE_SIZE,        String.valueOf(DEFAULT_PAGE_SIZE));
        values.put(ExtractorConstants.BISECT_FACTOR, String.valueOf(testBisectFactor));

        writeConfig.setValues(values);


        return writeConfig;

    }

    @Override
    protected ExtractorConfig<Cql3TestEntity> initWriteConfig() {

        ExtractorConfig<Cql3TestEntity> writeConfig = new ExtractorConfig<>(Cql3TestEntity.class);

        Map<String, String> values = new HashMap<>();
        values.put(ExtractorConstants.HOST,    Constants.DEFAULT_CASSANDRA_HOST);
        values.put(ExtractorConstants.KEYSPACE, AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME);
        values.put(ExtractorConstants.COLUMN_FAMILY,  AbstractDeepSparkContextTest.CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
        values.put(ExtractorConstants.CQLPORT,  String.valueOf(CassandraServer.CASSANDRA_CQL_PORT));
        values.put(ExtractorConstants.RPCPORT, String.valueOf(CassandraServer.CASSANDRA_THRIFT_PORT));
        values.put(ExtractorConstants.PAGE_SIZE,        String.valueOf(DEFAULT_PAGE_SIZE));
        values.put(ExtractorConstants.CREATE_ON_WRITE, String.valueOf(Boolean.TRUE));

        writeConfig.setValues(values);


        return writeConfig;

    }

    @Override
    public void testSaveToCassandra() {
        Function1<Cql3TestEntity, Cql3TestEntity> mappingFunc = new TestEntityAbstractSerializableFunction();
//        ClassTag$.MODULE$.<T>apply(config
//                .getEntityClass())
        //RDD<Cql3TestEntity> mappedRDD = getRDD().map(mappingFunc, ClassTag$.MODULE$.<Cql3TestEntity>apply(Cql3TestEntity.class));

        try {
            AbstractDeepSparkContextTest.executeCustomCQL("DROP TABLE " + AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME + "." + AbstractDeepSparkContextTest.CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
        } catch (Exception e) {
        }

       // assertTrue(mappedRDD.count() > 0);

        ExtractorConfig<Cql3TestEntity> writeConfig = getWriteConfig();
        //writeConfig.createTableOnWrite(Boolean.FALSE);

        try {
            //CassandraRDD.saveRDDToCassandra(mappedRDD, writeConfig);
            fail();
        } catch (DeepIOException e) {
            // ok
            //writeConfig.createTableOnWrite(Boolean.TRUE);
        }

        //CassandraRDD.saveRDDToCassandra(mappedRDD, writeConfig);
        checkOutputTestData();
    }

    @Override
    public void testSimpleSaveToCassandra() {
        try {
            AbstractDeepSparkContextTest.executeCustomCQL("DROP TABLE " + AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME + "." + AbstractDeepSparkContextTest.CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        ExtractorConfig<Cql3TestEntity> writeConfig = getWriteConfig();
        //writeConfig.createTableOnWrite(Boolean.FALSE);

        try {
          //  CassandraRDD.saveRDDToCassandra(getRDD(), writeConfig);
            fail();
        } catch (DeepIOException e) {
            // ok
            //writeConfig.createTableOnWrite(Boolean.TRUE);
        }

//        CassandraRDD.saveRDDToCassandra(getRDD(), writeConfig);

        checkSimpleTestData();

    }

    @Override
    public void testCql3SaveToCassandra() {
        try {
            AbstractDeepSparkContextTest.executeCustomCQL("DROP TABLE " + AbstractDeepSparkContextTest.OUTPUT_KEYSPACE_NAME + "." + AbstractDeepSparkContextTest.CQL3_ENTITY_OUTPUT_COLUMN_FAMILY);
        } catch (Exception e) {
        }

        ExtractorConfig<Cql3TestEntity> writeConfig = getWriteConfig();

        //CassandraRDD.cql3SaveRDDToCassandra(getRDD(), writeConfig);
        checkSimpleTestData();
    }
}