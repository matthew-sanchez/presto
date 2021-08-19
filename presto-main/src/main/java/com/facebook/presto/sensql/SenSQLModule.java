/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sensql;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;

import java.util.*;

import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static java.util.Objects.requireNonNull;

public class SenSQLModule
{
    private Map<String, Map<String, List<String>>> localdb;

    public SenSQLModule(Map<String, Map<String, List<String>>> map)
    {
        this.localdb = map;
    }
    public QueryPreparer.PreparedQuery rewrite(QuerySpecification originalBody, QueryPreparer queryPreparer, Session session, WarningCollector warningCollector)
    {
        // get relation name from 'from' clause
        if (originalBody.from.isPresent()) {
            originalBody.from = Optional.of(processFrom(originalBody.from.get()));
        }

        String from = ((Table) originalBody.from.get()).getName().toString();

        // get location from 'where' clause
        List<String> whereList = new LinkedList<>();
        if (originalBody.where.isPresent()) {
            originalBody.where = Optional.of(processWhere((LogicalBinaryExpression) originalBody.where.get(), whereList));
        }
        // default location = 'Morningside Heights' if none found
        if (whereList.size() == 0) {
            whereList.add("Morningside Heights");
        }

        // create 'with' clause
        // With{[WithQuery{Union}]}
        WithQuery[] withquery = new WithQuery[1];
        if (localdb.get(whereList.get(0)).get(from).size() == 1) {
            withquery[0] = new WithQuery(new Identifier(from), (Query) queryPreparer.sqlParser.createStatement("select * from " + localdb.get(whereList.get(0)).get(from).get(0) + "." + from, createParsingOptions(session, warningCollector)), Optional.empty());
        }
        else {
            List<Relation> unionRelations = new LinkedList<>();
            for (String db : localdb.get(whereList.get(0)).get(from)) {
                unionRelations.add(((Query) queryPreparer.sqlParser.createStatement("select * from " + db + "." + from, createParsingOptions(session, warningCollector))).queryBody);
            }
            Union union = new Union(unionRelations, Optional.of(false));
            withquery[0] = new WithQuery(new Identifier(from), new Query(Optional.empty(), union, Optional.empty(), Optional.empty(), Optional.empty()), Optional.empty());
        }
        With with = new With(false, Arrays.asList(withquery));

        Query newQuery = new Query(Optional.of(with), originalBody, Optional.empty(), Optional.empty(), Optional.empty());

        return queryPreparer.prepareQuery(session, newQuery, warningCollector);
    }

    private Table processFrom(Relation from)
    {
        Table result = new Table(QualifiedName.of(""));
        try {
            result = (Table) from;

            return result;
        }
        catch (ClassCastException e) {
        }
        try {
            Join join = (Join) from;
            if (((Table) join.getRight()).getName().toString().equals("map")) {
                result = new Table(QualifiedName.of(((Table) join.getLeft()).getName().toString()));
            }
            if (((Table) join.getLeft()).getName().toString().equals("map")) {
                result = new Table(QualifiedName.of(((Table) join.getRight()).getName().toString()));
            }
        }
        catch (ClassCastException e) {
        }
        return result;
    }

    private Expression processWhere(LogicalBinaryExpression expression, List<String> whereList)
    {
        try {
            try {
                expression.left = processWhere((LogicalBinaryExpression) expression.left, whereList);
            }
            catch (ClassCastException e) {
            }
            try {
                expression.right = processWhere((LogicalBinaryExpression) expression.right, whereList);
            }
            catch (ClassCastException e) {
            }
            boolean left = checkWhere(expression.left, whereList);
            boolean right = checkWhere(expression.right, whereList);
            if (left && right) {
                if (expression.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
                    return new BooleanLiteral(true);
                }
                else {
                    return new BooleanLiteral(false);
                }
            }
            if (left) {
                return expression.right;
            }
            if (right) {
                return expression.left;
            }
            return expression;
        }
        catch (ClassCastException e) {
            return expression;
        }
    }

    private boolean checkWhere(Expression where, List<String> whereList)
    {
        try {
            if (((FunctionCall) where).getName().toString().equals("st_contains")) {
                return true;
            }
        }
        catch (ClassCastException e) {
        }
        try {
            if (((ComparisonExpression) where).getOperator().equals(ComparisonExpression.Operator.EQUAL)) {
                if (((ComparisonExpression) where).getLeft().toString().equals("map.name")) {
                    whereList.add(((StringLiteral) (((ComparisonExpression) where).getRight())).getValue());
                    return true;
                }
            }
        }
        catch (ClassCastException e) {
        }
        return false;
    }

    private Statement rewrite(Statement node, SqlParser sqlParser, Session session, WarningCollector warningCollector)
    {
        return (Statement) new Visitor(session, sqlParser, warningCollector).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final SqlParser parser;
        private final WarningCollector warningCollector;

        public Visitor(
                Session session,
                SqlParser parser,
                WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.parser = parser;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitQuery(Query node, Void context)
                throws SemanticException
        {
//            System.out.println("visit query");
            for (Node child : node.getChildren()) {
                process(child);
            }
            return node;
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        @Override
        protected Relation visitRelation(Relation node, Void context)
        {
//            System.out.println("visit relation: " + node.toString());
            return node;
        }

        @Override
        protected QueryBody visitQueryBody(QueryBody node, Void context)
        {
//            System.out.println("visit query body");
            return node;
        }

        @Override
        protected QuerySpecification visitQuerySpecification(QuerySpecification node, Void context)
        {
//            System.out.println("visit query specification");
            return node;
        }
    }
}
