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

package com.stratio.deep.jdbc.extractor;

import static com.stratio.deep.commons.utils.Utils.initConfig;

import com.stratio.deep.commons.config.BaseConfig;
import com.stratio.deep.commons.exception.DeepGenericException;
import com.stratio.deep.commons.querybuilder.UpdateQueryBuilder;
import com.stratio.deep.commons.rdd.IExtractor;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.jdbc.reader.IJdbcReader;
import com.stratio.deep.jdbc.reader.JdbcReader;
import com.stratio.deep.jdbc.writer.JdbcWriter;
import org.apache.spark.Partition;
import org.apache.spark.rdd.JdbcPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Abstract class of Jdbc native extractor.
 */
public abstract class JdbcNativeExtractor<T, S extends BaseConfig> implements IExtractor<T, S> {

    /**
     * The serialUID.
     */
    private static final long serialVersionUID = -298383130965427783L;

    /**
     * The log.
     */
    private  static final Logger LOG = LoggerFactory.getLogger(JdbcNativeExtractor.class);

    /**
     * Jdbc Deep Job configuration.
     */
    protected JdbcDeepJobConfig<T> jdbcDeepJobConfig;

    /**
     * Jdbc reader
     */
    protected IJdbcReader jdbcReader;

    /**
     * Jdbc writer
     */
    protected JdbcWriter<T> jdbcWriter;

    /**
     * {@inheritDoc}
     */
    @Override
    public Partition[] getPartitions(S config) {
        LOG.info("recovered partitions");
        jdbcDeepJobConfig = initConfig(config, jdbcDeepJobConfig);

        int upperBound = jdbcDeepJobConfig.getUpperBound();
        int lowerBound = jdbcDeepJobConfig.getLowerBound();
        int numPartitions = jdbcDeepJobConfig.getNumPartitions();
        int length = 1 + upperBound - lowerBound;
        Partition [] result = new Partition[numPartitions];
        if (LOG.isDebugEnabled()){
            LOG.debug(String.format("Creating JDBC Partition with options upperBound [%s], lowerBound:[%s], numPartitions: [%s], length [%s]",upperBound,lowerBound,numPartitions,length));
        }
        for(int i=0; i<numPartitions; i++) {
            int start = lowerBound + lowerBound + ((i * length) / numPartitions);
            int end = lowerBound + (((i + 1) * length) / numPartitions) - 1;
            result[i] = new JdbcPartition(i, start, end);

            if (LOG.isDebugEnabled()){
                LOG.debug(String.format("new  JDBC Partition start [%s], end:[%s]",start,end));
            }
        }
        return result;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        try {
            return jdbcReader.hasNext();
        } catch (SQLException e) {
            String message = "A SQL Exception happens when we ask for nextElement."+e.toString();
            LOG.error(message);
            throw new DeepGenericException(message,e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T next() {
        try {
            return transformElement(jdbcReader.next());
        } catch (SQLException e) {
            String message = "A SQL Exception happens when we recover nextElement."+e.toString();
            LOG.error(message);
            throw new DeepGenericException(message,e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        closeJDBCReader();
        closeJDBCWriter();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void initIterator(Partition dp, S config) {
        jdbcDeepJobConfig = initConfig(config, jdbcDeepJobConfig);
        jdbcReader = new JdbcReader(jdbcDeepJobConfig);
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Init iterator with jdbcReader: [%s] and partitions [%s]",jdbcReader,dp));
            }
            jdbcReader.init(dp);
        } catch(Exception e) {
            String message = "Unable to initialize JdbcReader."+e.toString();
            LOG.error(message,e);
            

            throw new DeepGenericException(message, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveRDD(T t) {
             this.jdbcWriter.save(transformElement(t));

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getPreferredLocations(Partition split) {
        return Collections.EMPTY_LIST;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initSave(S config, T first, UpdateQueryBuilder queryBuilder) {

        jdbcDeepJobConfig = initConfig(config, jdbcDeepJobConfig);
        this.jdbcWriter = new JdbcWriter<>(jdbcDeepJobConfig);

    }


    protected abstract T transformElement(Map<String, Object> entity);

    protected abstract Map<String, Object> transformElement(T entity);

    /**
     * Close the JDBCWritter.
     */
    private void closeJDBCWriter() {
        if(jdbcWriter != null) {
            try {
                jdbcWriter.close();
            } catch(Exception e) {
                LOG.error("Unable to close jdbcWriter", e);
            }
        }
    }

    /**
     * Close the JDBCReader.
     */
    private void closeJDBCReader() {
        if(jdbcReader != null) {
            try {
                jdbcReader.close();
            } catch(Exception e) {
                LOG.error("Unable to close jdbcReader", e);
            }
        }
    }


}
