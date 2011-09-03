/**********************************************************************
Copyright (c) 2011 Pulasthi Supun. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/
package com.spidertracks.datanucleus.query;

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getDiscriminatorColumnName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.jdo.identity.SingleFieldIdentity;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.KeyRange;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.query.evaluator.JPQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.CassandraStoreManager;
import com.spidertracks.datanucleus.client.Consistency;
import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.query.runtime.Columns;
import com.spidertracks.datanucleus.query.runtime.Operand;
import com.spidertracks.datanucleus.utils.FilterUtils;
import com.spidertracks.datanucleus.utils.MetaDataUtils;

/**
 * Handles JPQL queries.
 * @version $Id$
 */
public class JPQLQuery extends AbstractJPQLQuery
{

    /**
     * default maximum.
     */
    private static int DEFAULT_MAX = 1000;

    /**
     * serial version id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * boolean to test whether the query is indexed or non-indexed.
     */
    private boolean nonIndexedQuery = true;

    /**
     * Cassandra expression evaluator.
     */
    private CassandraQueryExpressionEvaluator evaluator;

    /**
     * Creates a new query instance with the given execution context.
     * @param ec the associated execution context for this query
     */
    public JPQLQuery(ExecutionContext ec) {
        super(ec, (JPQLQuery) null);
        // TODO Auto-generated constructor stub
    }

    /**
     * Creates a new query instance with the given execution context and the
     * given JPQLQuery.
     * @param ec the associated execution context
     * @param q the associated JPQLQuery
     */
    public JPQLQuery(ExecutionContext ec, JPQLQuery q) {
        super(ec, q);
    }

    /**
     * Creates an new query instance with the given execution context and the
     * given string query.
     * @param ec the associated execution context
     * @param query the associated string query
     */
    public JPQLQuery(ExecutionContext ec, String query) {
        super(ec, query);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Object performExecute(Map parameters) {

        long startTime = System.currentTimeMillis();

        if (NucleusLogger.QUERY.isDebugEnabled()) {
            NucleusLogger.QUERY.debug(
                    LOCALISER.msg("0121121", "JPQL", getSingleStringQuery()),
                    null);

        }

        Expression filter = this.getCompilation().getExprFilter();

        String poolName = ((CassandraStoreManager) ec.getStoreManager())
                .getPoolName();

        ByteConverterContext byteContext = ((CassandraStoreManager) ec
                .getStoreManager()).getByteConverterContext();

        AbstractClassMetaData acmd = ec.getMetaDataManager()
                .getMetaDataForClass(candidateClass.getName(),
                        ec.getClassLoaderResolver());

        String columnFamily = MetaDataUtils.getColumnFamily(acmd);

        Set<Columns> candidateKeys = null;

        Bytes idColumnBytes = MetaDataUtils.getIdentityColumn(acmd);
        DiscriminatorMetaData discriminator = null;

        ClassLoaderResolver clr = ec.getClassLoaderResolver();

        Bytes[] selectColumns = null;
        Bytes descriminiatorCol = null;

        if (acmd.hasDiscriminatorStrategy()) {

            discriminator = acmd.getDiscriminatorMetaData();

            descriminiatorCol = getDiscriminatorColumnName(discriminator);

            selectColumns = new Bytes[]{idColumnBytes, descriminiatorCol };
        } else {
            selectColumns = new Bytes[]{idColumnBytes };
        }

        int range = DEFAULT_MAX;

        if (this.getRange() != null) {
            range = (int) this.getRangeToExcl();

            if (this.getOrdering() == null) {
                throw new NucleusDataStoreException(
                        "You cannot invoke a without an ordering expression against Cassandra. Results will be "
                                + "randomly ordered from Cassnadra and need order to page");

            }
        }

        evaluator = new CassandraQueryExpressionEvaluator(acmd, range,
                byteContext, parameters);
        if (filter != null) {
            FilterUtils filterutils = new FilterUtils();
            nonIndexedQuery = filterutils.checkFilterValidity(filter, candidateClass, evaluator);
        }

        if (filter != null && !nonIndexedQuery) {
            Operand opTree = (Operand) filter.evaluate(evaluator);

            // there's a discriminator so be sure to include it
            if (acmd.hasDiscriminatorStrategy()) {
                List<Bytes> descriminatorValues = MetaDataUtils
                        .getDescriminatorValues(acmd.getFullClassName(), clr,
                                ec, byteContext);

                opTree = opTree.optimizeDescriminator(descriminiatorCol,
                        descriminatorValues);
            }
            // perform a query rewrite to take into account descriminator values
            opTree.performQuery(poolName, columnFamily, selectColumns);

            candidateKeys = opTree.getCandidateKeys();
        } else {
            candidateKeys = getAll(poolName, columnFamily, selectColumns, range);
        }
        Collection<?> results = getObjectsOfCandidateType(candidateKeys, acmd,
                clr, subclasses, idColumnBytes, descriminiatorCol, byteContext);

        if (this.getOrdering() != null || this.getGrouping() != null
                || nonIndexedQuery) {

            // Apply any result restrictions to the results
            JavaQueryEvaluator resultMapper = new JPQLEvaluator(this, results,
                    compilation, parameters, ec.getClassLoaderResolver());

            results = resultMapper.execute(true, true, true, true, true);

        }

        if (NucleusLogger.QUERY.isDebugEnabled()) {
            NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JPQL", ""
                    + (System.currentTimeMillis() - startTime)));
        }

        return results;
    }

    /**
     * Used to load specific keys.
     * @param keys set of keys
     * @param acmd AbstractClassMetaData object
     * @param clr ClassLoaderResolver
     * @param subclasses a boolean to specify whether this is a sub class
     * @param identityColumn identityColumn Bytes
     * @param descriminatorColumn descriminatorColumn Bytes
     * @param byteConverter ByteConverterContext
     * @return returns the list of objects of candidate types
     */
    public List<?> getObjectsOfCandidateType(Set<Columns> keys,
            AbstractClassMetaData acmd, ClassLoaderResolver clr,
            boolean subclasses, Bytes identityColumn,
            Bytes descriminatorColumn, ByteConverterContext byteConverter) {

        List<Object> results = new ArrayList<Object>(keys.size());

        for (Columns idBytes : keys) {

            Class<?> targetClass = candidateClass;

            if (descriminatorColumn != null) {

                String descriminatorValue = byteConverter.getString(idBytes
                        .getColumnValue(descriminatorColumn));

                String className = org.datanucleus.metadata.MetaDataUtils
                        .getClassNameFromDiscriminatorValue(descriminatorValue,
                                acmd.getDiscriminatorMetaData(), ec);

                targetClass = clr.classForName(className);

            }

            Object identity = byteConverter.getObjectIdentity(ec, targetClass,
                    idBytes.getColumnValue(identityColumn));

            // Not a valid subclass, don't return it as a candidate
            if (!(identity instanceof SingleFieldIdentity)) {
                throw new NucleusDataStoreException(
                        "Only single field identities are supported");
            }

            if (!ClassUtils.typesAreCompatible(candidateClass,
                    ((SingleFieldIdentity) identity).getTargetClassName(), clr)) {
                continue;
            }

            Object returned = ec.findObject(identity, true, subclasses,
                    candidateClass.getName());

            if (returned != null) {
                results.add(returned);
            }
        }

        return results;

    }

    /**
     * Get all keys from a given column family. Used ranges to set the max
     * amount.
     * @param poolName The pool name
     * @param cfName column family name
     * @param selectColumns columns byte array
     * @param maxSize maximum size
     * @return returns a set of columns
     */
    private Set<Columns> getAll(String poolName, String cfName,
            Bytes[] selectColumns, int maxSize) {

        Set<Columns> candidateKeys = new HashSet<Columns>();

        KeyRange range = new KeyRange();
        range.setStart_key(new byte[] {});
        range.setEnd_key(new byte[] {});
        range.setCount(maxSize);

        Map<Bytes, List<Column>> results;

        try {
            results = Pelops.createSelector(poolName).getColumnsFromRows(
                    cfName, range, Selector.newColumnsPredicate(selectColumns),
                    Consistency.get());
        } catch (Exception e) {
            throw new NucleusException("Error scanning rows", e);
        }

        Columns cols;

        for (Entry<Bytes, List<Column>> entry : results.entrySet()) {

            if (entry.getValue().size() == 0) {
                continue;
            }

            cols = new Columns(entry.getKey());

            for (Column currentCol : entry.getValue()) {

                cols.addResult(currentCol);
            }

            candidateKeys.add(cols);
        }

        return candidateKeys;
    }

}
