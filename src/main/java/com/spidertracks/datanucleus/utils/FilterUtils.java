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
package com.spidertracks.datanucleus.utils;

import java.util.List;

import org.datanucleus.query.expression.Expression;

import com.spidertracks.datanucleus.query.AnnotationEvaluator;
import com.spidertracks.datanucleus.query.CassandraQueryExpressionEvaluator;

/**
 * Utility class to evaluate Expressions.
 * @version $Id$
 */
public class FilterUtils
{
    /**
     * checks whether the filter has only non indexed fields.
     * @param filter Expression filter
     * @param candidateClass The candidate class
     * @param evaluator CassandraQueryExpressionEvaluator
     * @return returns true if the given expression is a non-indexed query and false if not.
     */
    public boolean checkFilterValidity(Expression filter, Class<?> candidateClass,
            CassandraQueryExpressionEvaluator evaluator) {

        boolean nonIndexedQuery = true;
        AnnotationEvaluator ae = new AnnotationEvaluator(candidateClass);
        List<String> annotationlist = ae.getAnnotatedFields("javax.jdo.annotations.Index");

        List<String> expressionlist = evaluator.getPrimaryExpressions(filter);
        if (annotationlist == null) {
            nonIndexedQuery = true;
            return nonIndexedQuery;
        }
        if (expressionlist == null) {
            return nonIndexedQuery;
        }
        for (String ex : expressionlist) {
            if (annotationlist.indexOf(ex) != -1) {
                nonIndexedQuery = false;
                return nonIndexedQuery;
            }
        }
        return nonIndexedQuery;
    }
}
