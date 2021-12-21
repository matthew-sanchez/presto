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
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.BooleanLiteral;
import org.postgresql.PGConnection;
import sun.jvm.hotspot.types.basic.BasicOopField;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.AstVisitor;

import sun.rmi.runtime.Log;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Connection;
//import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;

import org.postgresql.Driver;

import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static java.util.Objects.requireNonNull;

public class SenSQLModule
{
    private Map<String, Map<String, List<String>>> localdb;
    private Connection localDB;
    private Driver driver;
   // private List<Expression> forwardClauses;

    public SenSQLModule(Map<String, Map<String, List<String>>> map)
    {
        System.out.println("Init SenSQLModule");
        this.localdb = map;
        try {
            System.out.println("attempting localDB connection");
            this.driver = new Driver();
            this.localDB = this.driver.connect("jdbc:postgresql://db.sece.io/geonaming?"
                        + "user=geo_api"
                        + "&password=g3QMmAsv"
                        + "&sslmode=require", new Properties());
            System.out.println("\nlocalDB connection: ");
            System.out.println(this.localDB);
            java.sql.Statement s = this.localDB.createStatement();
            s.executeUpdate("set session.uid='R2wzx2ovqEeBSjcRxgPZ9ZNT2j33';");
            s.close();

        }
        catch (SQLException e) {
            System.out.println("FAILED TO CONNECT TO LOCALDB");
            e.printStackTrace();
        }

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

        Statement localQuery = null;

        if (originalBody.where.isPresent()) {
            // make copy for forward query
            localQuery = queryPreparer.sqlParser.createStatement("select nodes.id from\n" +
                    "nodes, feature, shape where\n" +
                    /*"st_intersects(shape.geometries, nodes.service_region) and*/" shape.id = feature.shape and "
                    + originalBody.where.get()) ;

            // process where clause for presto query
//            System.out.println("original where: " + originalBody.where);
//            System.out.println("-- beginning recursion --");
            originalBody.where = Optional.of(processWhere((LogicalBinaryExpression) originalBody.where.get(), new LinkedList<>()));
//            System.out.println("-- end recursion --");
            if (originalBody.where.get() instanceof BooleanLiteral) {
                originalBody.where = Optional.empty();
            }
            System.out.println("modified where: " + originalBody.where);
        }
        // now start buildling the localdb query
//        System.out.println("original localQuery: " + ((Query) localQuery).queryBody.toString());
        String forwardString = "";
        if (localQuery != null) {
            QuerySpecification forwardBody = ((QuerySpecification) ((((Query) localQuery).getQueryBody())));

//            System.out.println("-- beginning recursion --");
            forwardBody.where = Optional.of(processWhereBackend(forwardBody.where.get()));
//            System.out.println("-- end recursion --");
            forwardString = "select " + forwardBody.select.getSelectItems().get(0)
                    + " from " + ((Table) ((Join) ((Join) forwardBody.from.get()).getLeft()).getLeft()).getName() + ", "
                    + ((Table) ((Join) ((Join) forwardBody.from.get()).getLeft()).getRight()).getName() + ", "
                    + ((Table) ((Join) forwardBody.from.get()).getRight()).getName()
                    + " where " + forwardBody.where.get() + " group by " + forwardBody.select.getSelectItems().get(0);

            // exec localdb query and build wherelist from results
            try {
                System.out.println("executing localDB query: [" + forwardString + "]");
                java.sql.Statement s = this.localDB.createStatement();
                ResultSet rs = s.executeQuery(forwardString);
                while (rs.next()) {
//                    System.out.println("row: " + rs.getString(1));
                    whereList.add(rs.getString(1));
                }
            }
            catch (SQLException sqle) {
                sqle.printStackTrace();
            }
        }

        System.out.print("catalogs from localDB: " + whereList);
        // default location = 'Morningside Heights' if none found
        if (whereList.size() == 0) {
            whereList.add("Morningside Heights");
        }

//        System.out.println("forwardList: " + this.forwardClauses);
//        QuerySpecification localBody = ((QuerySpecification) ((((Query) localQuery).getQueryBody())));
//        System.out.println(localBody);

        // create 'with' clause
        // With{[WithQuery{Union}]}
        WithQuery[] withquery = new WithQuery[1];
        List<Relation> unionRelations = new LinkedList<>();
        for (String s : whereList) {
            // add select * from [id] for every id returned from localDB
            unionRelations.add(((Query) queryPreparer.sqlParser.createStatement(
                    "select * from " + s + ".public." + from,
                    createParsingOptions(session, warningCollector))).queryBody);
        }

        if (unionRelations.size() > 1) {
            Union union = new Union(unionRelations, Optional.of(false));
            withquery[0] = new WithQuery(new Identifier(from), new Query(Optional.empty(), union, Optional.empty(), Optional.empty(), Optional.empty()), Optional.empty());
        }
        else {
            withquery[0] = new WithQuery(new Identifier(from), new Query(Optional.empty(), (QueryBody) unionRelations.get(0), Optional.empty(), Optional.empty(), Optional.empty()), Optional.empty());
        }
        With with = new With(false, Arrays.asList(withquery));

        Query newQuery = new Query(Optional.of(with), originalBody, Optional.empty(), Optional.empty(), Optional.empty());

//        System.out.println(newQuery.queryBody);

        return queryPreparer.prepareQuery(session, newQuery, warningCollector);
    }

    private Table processFrom(Relation from)
    {
        // Right now just supports two tables in original query
        Table result = new Table(QualifiedName.of(""));
        try {
            result = (Table) from;

            return result;
        }
        catch (ClassCastException e) {
        }
        try {
            Join join = (Join) from;
            if (((Table) join.getRight()).getName().toString().equals("feature")) {
                result = new Table(QualifiedName.of(((Table) join.getLeft()).getName().toString()));
            }
            if (((Table) join.getLeft()).getName().toString().equals("feature")) {
                result = new Table(QualifiedName.of(((Table) join.getRight()).getName().toString()));
            }
        }
        catch (ClassCastException e) {
        }
        return result;
    }

    private Expression processWhere(Expression expression, List<String> whereList)
    {
//        System.out.println(expression.toString());

        try {
            LogicalBinaryExpression lbExpression = (LogicalBinaryExpression) expression;

            try {
                lbExpression.right = processWhere(lbExpression.right, whereList);
            }
            catch (ClassCastException e) {
            }

            try {
                lbExpression.left = processWhere(lbExpression.left, whereList);
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
                    }
                    else {
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
        if (checkCond(expression, whereList)) {
            return new BooleanLiteral(true);
        }
        return expression;
    }

    private boolean checkCond(Expression condition, List<String> whereList)
    {

        try {
            if (!(condition instanceof LogicalBinaryExpression)) {
//                System.out.println("checking condition: " + condition.toString());
//                System.out.println("checking for 'feature': " + condition.toString());
                // This currently also removes st_*() clauses, intentional while we aren't yet geoquerying nodes
                if (condition.toString().contains("feature")) {
//                    System.out.println("found");
//                    this.forwardClauses.add(Condition);
//                    if (!(condition instanceof NotExpression)) {
//                        whereList.add(((StringLiteral) (((ComparisonExpression) condition).getRight())).getValue());
//                    }
                    return true;
                }
            }
        }
        catch (ClassCastException e) {
        }
        return false;
    }

    private Expression processWhereBackend(Expression expression)
    {
//        System.out.println(expression.toString());
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
                    }
                    else {
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
//                System.out.println("checking condition: " + condition.toString());
//                if (condition.toString().contains("measurements")) {
//                    System.out.println("contains 'measurements'");
//                }
                if ((!condition.toString().contains("feature") && !condition.toString().contains("shape"))
                        || condition.toString().contains("measurements") || condition.toString().contains("st_")) {
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

    // attempt to write rewrite as a visitor operation...
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
