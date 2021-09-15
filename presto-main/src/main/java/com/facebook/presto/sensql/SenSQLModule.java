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
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.*;
import sun.jvm.hotspot.types.basic.BasicOopField;
import sun.rmi.runtime.Log;

import java.text.ParseException;
import java.util.*;

import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static java.util.Objects.requireNonNull;

public class SenSQLModule
{
    private Map<String, Map<String, List<String>>> localdb;
   // private List<Expression> forwardClauses;

    public SenSQLModule(Map<String, Map<String, List<String>>> map)
    {
        this.localdb = map;
//        this.forwardClauses = new LinkedList<>();
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

        Statement forwardQuery = null;

        if (originalBody.where.isPresent()) {
            forwardQuery = queryPreparer.sqlParser.createStatement("select acps.catalog from\n" +
                    "acps, features where\n" +
                    "st_intersects(features.region, acps.service_region) and " + originalBody.where.get());
            System.out.println("original where: " + originalBody.where);
            System.out.println("-- beginning recursion --");
            originalBody.where = Optional.of(processWhere((LogicalBinaryExpression) originalBody.where.get(), whereList));
            System.out.println("-- end recursion --");
            if (originalBody.where.get() instanceof BooleanLiteral) {
                originalBody.where = Optional.empty();
            }
            System.out.println("modified where: " + originalBody.where);
        }
        System.out.println("original forwardQuery: " + forwardQuery);
        if( forwardQuery != null) {
            QuerySpecification forwardBody = ((QuerySpecification) ((((Query) forwardQuery).getQueryBody())));
            System.out.println("-- beginning recursion --");
            forwardBody.where = Optional.of(processWhereBackend((LogicalBinaryExpression) forwardBody.where.get()));
            System.out.println("-- end recursion --");
        }
        System.out.println("modified forwardQuery: " + forwardQuery);




        System.out.print(whereList);
        // default location = 'Morningside Heights' if none found
        if (whereList.size() == 0) {
            whereList.add("Morningside Heights");
        }

//        System.out.println("forwardList: " + this.forwardClauses);


        QuerySpecification forwardBody = ((QuerySpecification) ((((Query) forwardQuery).getQueryBody())));
        // build tree and set to right
//        if (this.forwardClauses.size() == 0) {
//
//            throw new RuntimeException();
//        }
//        else if (this.forwardClauses.size() == 1) {
//            ((LogicalBinaryExpression) (forwardBody.where.get())).right = this.forwardClauses.get(0);
//        }
//        else {
//            Expression baseWhere = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, this.forwardClauses.get(0), this.forwardClauses.get(1));
//            for (int i = 2; i < this.forwardClauses.size(); i++) {
//                baseWhere = new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND, baseWhere, this.forwardClauses.get(i));
//            }
//            ((LogicalBinaryExpression) (forwardBody.where.get())).right = baseWhere;
//        }
        System.out.println(forwardBody);



        // create 'with' clause
        // With{[WithQuery{Union}]}
        WithQuery[] withquery = new WithQuery[1];
        List<Relation> unionRelations = new LinkedList<>();


        for (int i = 0; i < whereList.size(); i++) {
//        if (localdb.get(whereList.get(i)).get(from).size() == 1) {
//            withquery[0] = new WithQuery(new Identifier(from), (Query) queryPreparer.sqlParser.createStatement("select * from " + localdb.get(whereList.get(0)).get(from).get(0) + "." + from, createParsingOptions(session, warningCollector)), Optional.empty());
//        }
//        else {
            for (String db : localdb.get(whereList.get(i)).get(from)) {
                unionRelations.add(((Query) queryPreparer.sqlParser.createStatement("select * from " + db + "." + from, createParsingOptions(session, warningCollector))).queryBody);
            }
        }

        if(unionRelations.size() > 1) {
            Union union = new Union(unionRelations, Optional.of(false));
            withquery[0] = new WithQuery(new Identifier(from), new Query(Optional.empty(), union, Optional.empty(), Optional.empty(), Optional.empty()), Optional.empty());
        }
        else {
            withquery[0] = new WithQuery(new Identifier(from), new Query(Optional.empty(), (QueryBody) unionRelations.get(0), Optional.empty(), Optional.empty(), Optional.empty()), Optional.empty());
        }
        With with = new With(false, Arrays.asList(withquery));

        Query newQuery = new Query(Optional.of(with), originalBody, Optional.empty(), Optional.empty(), Optional.empty());

        return queryPreparer.prepareQuery(session, newQuery, warningCollector);
    }

    private Table processFrom(Relation from)
    {
        // Right now, just supports two tables in original query
        Table result = new Table(QualifiedName.of(""));
        try {
            result = (Table) from;

            return result;
        }
        catch (ClassCastException e) {
        }
        try {
            Join join = (Join) from;
            if (((Table) join.getRight()).getName().toString().equals("features")) {
                result = new Table(QualifiedName.of(((Table) join.getLeft()).getName().toString()));
            }
            if (((Table) join.getLeft()).getName().toString().equals("features")) {
                result = new Table(QualifiedName.of(((Table) join.getRight()).getName().toString()));
            }
        }
        catch (ClassCastException e) {
        }
        return result;
    }

    private Expression processWhere(Expression expression, List<String> whereList)
    {
        //temp to get OR working for test purposes
//        if(expression.getOperator() == LogicalBinaryExpression.Operator.OR) {
//            return expression;
//        }

        System.out.println(expression.toString());

        try {
            LogicalBinaryExpression lbExpression = (LogicalBinaryExpression) expression;
            try {
                lbExpression.left = processWhere(lbExpression.left, whereList);
            }
            catch (ClassCastException e) {
            }
            try {
                lbExpression.right = processWhere(lbExpression.right, whereList);
            }
            catch (ClassCastException e) {
            }
            if (lbExpression.left instanceof BooleanLiteral && lbExpression.right instanceof BooleanLiteral) {
                return new BooleanLiteral(true);
            }
            else if (lbExpression.left instanceof BooleanLiteral) {
                return lbExpression.right;
            }
            else if (lbExpression.right instanceof BooleanLiteral) {
                return lbExpression.left;
            }
            else {
                boolean left = checkCond(lbExpression.left, whereList);
                boolean right = checkCond(lbExpression.right, whereList);
                if (left && right) {
                    if (lbExpression.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
                        return new BooleanLiteral(true);
                    } else {
                        return new BooleanLiteral(false);
                    }
                }
                if (left) {
                    return lbExpression.right;
                }
                if (right) {
                    return lbExpression.left;
                }
                return lbExpression;
            }
        }
        catch (ClassCastException e) {

        }
        try {
            NotExpression nExpression = (NotExpression) expression;
            if (checkCond(nExpression, whereList)) {
                return new BooleanLiteral(true);
            }
            else {
                return expression;
            }
        }
        catch (ClassCastException e) {

        }
        return expression;
    }

    private boolean checkCond(Expression condition, List<String> whereList)
    {
//        System.out.println(Condition.toString());
//        try {
//            if (((LogicalBinaryExpression) Condition).getOperator().equals(LogicalBinaryExpression.Operator.OR)) {
//                whereList.add(( (StringLiteral) ((ComparisonExpression)(((LogicalBinaryExpression) Condition).getRight())).getRight() ).getValue());
//                whereList.add(( (StringLiteral) ((ComparisonExpression)(((LogicalBinaryExpression) Condition).getLeft())).getRight() ).getValue());
//                System.out.println(Condition);
////                this.forwardClauses.add(Condition);
//                return true;
//            }
//        }
//        catch (ClassCastException e) {
//        }
//        try {
//            if (((FunctionCall) Condition).getName().toString().equals("st_contains")) {
//                this.forwardClauses.add(Condition);
//                return false;
//            }
//        }
//        catch (ClassCastException e) {
//        }
//        try {
//            if (((ComparisonExpression) Condition).getOperator().equals(ComparisonExpression.Operator.EQUAL)) {
//                if (((ComparisonExpression) Condition).getLeft().toString().equals("features.name")) {
////                    this.forwardClauses.add(Condition);
//                    whereList.add(((StringLiteral) (((ComparisonExpression) Condition).getRight())).getValue());
//                    return true;
//                }
//            }
//        }
        try {
            if (!(condition instanceof LogicalBinaryExpression)) {
//                System.out.println("checking condition: " + condition.toString());
//                System.out.println("checking for 'features': " + condition.toString());
                if (condition.toString().contains("features")) {

                    System.out.println("found");
//                    this.forwardClauses.add(Condition);
                    if(!(condition instanceof NotExpression)) {
                        whereList.add(((StringLiteral) (((ComparisonExpression) condition).getRight())).getValue());
                    }
                    return true;
                }
            }
        }
        catch (ClassCastException e) {
        }
        return false;
    }

    private Expression processWhereBackend(Expression expression) {
        System.out.println(expression.toString());
        try {
            LogicalBinaryExpression lbExpression = (LogicalBinaryExpression) expression;
            try {
                lbExpression.left = processWhereBackend(lbExpression.left);
            }
            catch (ClassCastException e) {
            }
            try {
                lbExpression.right = processWhereBackend(lbExpression.right);
            }
            catch (ClassCastException e) {
            }
            if (lbExpression.left instanceof BooleanLiteral && lbExpression.right instanceof BooleanLiteral) {
                return new BooleanLiteral(true);
            }
            else if (lbExpression.left instanceof BooleanLiteral) {
                return lbExpression.right;
            }
            else if (lbExpression.right instanceof BooleanLiteral) {
                return lbExpression.left;
            }
            else {
                boolean left = checkCondBackend(lbExpression.left);
                boolean right = checkCondBackend(lbExpression.right);
                if (left && right) {
                    if (lbExpression.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
                        return new BooleanLiteral(true);
                    } else {
                        return new BooleanLiteral(false);
                    }
                }
                if (left) {
                    return lbExpression.right;
                }
                if (right) {
                    return lbExpression.left;
                }
                return expression;
            }
        }
        catch (ClassCastException e) {

        }
        try {
            NotExpression nExpression = (NotExpression) expression;
            if (checkCondBackend(nExpression)) {
                return new BooleanLiteral(true);
            }
            else {
                return expression;
            }
        }
        catch (ClassCastException e) {

        }
        return expression;
    }

    private boolean checkCondBackend(Expression condition)
    {
        try {
            if (!(condition instanceof LogicalBinaryExpression)) {
                System.out.println("checking condition: " + condition.toString());
                if(condition.toString().contains("measurements")) {
                    System.out.println("contains 'measurements'");
                }
                if(!condition.toString().contains("features")  || condition.toString().contains("measurements")) {
//                    this.forwardClauses.add(Condition);
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
