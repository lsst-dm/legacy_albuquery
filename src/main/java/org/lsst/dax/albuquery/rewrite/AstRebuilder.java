/* This file is part of albuquery.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

package org.lsst.dax.albuquery.rewrite;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.facebook.presto.sql.tree.*;
import com.facebook.presto.sql.tree.Window;

public class AstRebuilder<C>
        extends AstVisitor<Node, C>
{

    public <T extends Node> T processNode(T node, C context){
        return (T) process(node, context);
    }

    public  <T extends Node> List<T> processNodeItems(List<T> nodeItems, C context){
        return nodeItems.stream()
                .map(value -> processNode(value, context))
                .collect(Collectors.toList());
    }

    @Override
    public Extract visitExtract(Extract node, C context)
    {
        return new Extract(/* node.getLocation().get() */
                processNode(node.getExpression(), context),
                node.getField()
        );
    }

    @Override
    public Identifier visitIdentifier(Identifier identifier, C context){
        return identifier;
    }

    @Override
    public Cast visitCast(Cast node, C context)
    {
        return new Cast(/* node.getLocation().get() */
                processNode(node.getExpression(), context),
                node.getType(),
                node.isSafe(),
                node.isTypeOnly()
        );
    }

    @Override
    protected ArithmeticBinaryExpression visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        return new ArithmeticBinaryExpression(/* node.getLocation().get() */
                node.getType(),
                processNode(node.getLeft(), context),
                processNode(node.getRight(), context));
    }

    /*
    @Override
    public Node visitLikeClause(LikeClause node, C context) {
        return null;
    }
    */

    @Override
    protected BetweenPredicate visitBetweenPredicate(BetweenPredicate node, C context)
    {
        return new BetweenPredicate(/* node.getLocation().get() */
                processNode(node.getValue(), context),
                processNode(node.getMin(), context),
                processNode(node.getMax(), context));
    }

    @Override
    protected CoalesceExpression visitCoalesceExpression(CoalesceExpression node, C context)
    {
        return new CoalesceExpression(/* node.getLocation().get() */
                processNodeItems(node.getOperands(), context));
    }

    @Override
    public Node visitCurrentTime(CurrentTime node, C context) {
        return node;
    }

    @Override
    protected AtTimeZone visitAtTimeZone(AtTimeZone node, C context) {
        return new AtTimeZone(/* node.getLocation().get() */
                processNode(node.getValue(), context),
                processNode(node.getTimeZone(), context)
        );
    }

    @Override
    protected ArrayConstructor visitArrayConstructor(ArrayConstructor node, C context)
    {
        return new ArrayConstructor(/* node.getLocation().get() */
                processNodeItems(node.getValues(), context));
    }

    @Override
    protected SubscriptExpression visitSubscriptExpression(SubscriptExpression node, C context)
    {
        return new SubscriptExpression(/* node.getLocation().get() */
                processNode(node.getBase(), context),
                processNode(node.getIndex(), context));
    }

    @Override
    protected ComparisonExpression visitComparisonExpression(ComparisonExpression node, C context)
    {
        return new ComparisonExpression(/* node.getLocation().get() */
                node.getType(),
                processNode(node.getLeft(), context),
                processNode(node.getRight(), context));
    }

    @Override
    public Node visitLiteral(Literal node, C context) {
        return node;
    }

    @Override
    protected Query visitQuery(Query node, C context)
    {
        Optional<With> with = Optional.empty();
        Optional<OrderBy> orderBy = Optional.empty();
        if (node.getWith().isPresent()) {
            with = Optional.of(processNode(node.getWith().get(), context));
        }
        QueryBody queryBody = processNode(node.getQueryBody(), context);
        if (node.getOrderBy().isPresent()) {
            orderBy = Optional.of(processNode(node.getOrderBy().get(), context));
        }
        return new Query(/* node.getLocation().get() */
                with,
                queryBody,
                orderBy,
                node.getLimit());
    }

    @Override
    protected With visitWith(With node, C context)
    {
        return new With(/* node.getLocation().get() */
                node.isRecursive(),
                processNodeItems(node.getQueries(), context));
    }

    @Override
    protected WithQuery visitWithQuery(WithQuery node, C context)
    {
        return new WithQuery(/* node.getLocation().get() */
                node.getName(),
                processNode(node.getQuery(), context),
                node.getColumnNames()
                );
    }

    @Override
    protected Select visitSelect(Select node, C context)
    {
        return new Select(/* node.getLocation().get() */
                node.isDistinct(),
                processNodeItems(node.getSelectItems(), context));
    }

    /*
    @Override
    public Node visitStatement(Statement node, C context) {
        return null;
    }

    @Override
    public Node visitRelation(Relation node, C context) {
        return null;
    }

    @Override
    public Node visitQueryBody(QueryBody node, C context) {
        return null;
    }
     */

    @Override
    protected SingleColumn visitSingleColumn(SingleColumn node, C context)
    {
        return new SingleColumn(/* node.getLocation().get() */
                processNode(node.getExpression(), context),
                node.getAlias());
    }

    @Override
    protected AllColumns visitAllColumns(AllColumns node, C context)
    {
        if(node.getPrefix().isPresent()){
            return new AllColumns(node.getPrefix().get());
        }
        return new AllColumns();
    }

    @Override
    protected WhenClause visitWhenClause(WhenClause node, C context)
    {
        return new WhenClause(/* node.getLocation().get() */
            processNode(node.getOperand(), context),
            processNode(node.getResult(), context));
    }

    @Override
    protected InPredicate visitInPredicate(InPredicate node, C context)
    {
        return new InPredicate(/* node.getLocation().get() */
            processNode(node.getValue(), context),
            processNode(node.getValueList(), context));
    }

    @Override
    protected FunctionCall visitFunctionCall(FunctionCall node, C context)
    {
        List<Expression> arguments = processNodeItems(node.getArguments(), context);
        Optional<OrderBy> orderBy = Optional.empty();
        Optional<Window> window = Optional.empty();
        Optional<Expression> filter = Optional.empty();

        if (node.getOrderBy().isPresent()) {
            orderBy = Optional.of(processNode(node.getOrderBy().get(), context));
        }

        if (node.getWindow().isPresent()) {
            window = Optional.of(processNode(node.getWindow().get(), context));
        }

        if (node.getFilter().isPresent()) {
            filter = Optional.of(processNode(node.getFilter().get(), context));
        }
        return new FunctionCall(/* node.getLocation().get() */
                node.getName(),
                window,
                filter,
                orderBy,
                node.isDistinct(),
                arguments);
    }

    @Override
    protected GroupingOperation visitGroupingOperation(GroupingOperation node, C context)
    {
        List<DereferenceExpression> columnArguments = node.getGroupingColumns().stream()
                .map(value -> processNode((DereferenceExpression) value, context))
                .collect(Collectors.toList());

        return new GroupingOperation(node.getLocation(),
                columnArguments.stream()
                        .map(DereferenceExpression::getQualifiedName)
                        .collect(Collectors.toList()));
    }

    @Override
    protected DereferenceExpression visitDereferenceExpression(DereferenceExpression node, C context)
    {
        return new DereferenceExpression(/* node.getLocation().get() */
                processNode(node.getBase(), context),
                node.getField());
    }

    @Override
    public Window visitWindow(Window node, C context)
    {
        List<Expression> partitionBys = processNodeItems(node.getPartitionBy(), context);

        Optional<OrderBy> orderBy = Optional.empty();
        Optional<WindowFrame> windowFrame = Optional.empty();

        if (node.getOrderBy().isPresent()) {
            orderBy = Optional.of(processNode(node.getOrderBy().get(), context));
        }

        if (node.getFrame().isPresent()) {
            windowFrame = Optional.of(processNode(node.getFrame().get(), context));
        }

        return new Window(/* node.getLocation().get() */
                partitionBys,
                orderBy,
                windowFrame);
    }

    @Override
    public  WindowFrame visitWindowFrame(WindowFrame node, C context)
    {
        Optional<FrameBound> end = Optional.empty();
        if (node.getEnd().isPresent()) {
            end = Optional.of(processNode(node.getEnd().get(), context));
        }
        return new WindowFrame(/* node.getLocation().get() */
                node.getType(),
                processNode(node.getStart(), context),
                end);
    }

    @Override
    public  FrameBound visitFrameBound(FrameBound node, C context)
    {

        Optional<Expression> value = Optional.empty();
        if (node.getValue().isPresent()) {
            value = Optional.of(processNode(node.getValue().get(), context));
        }

        return new FrameBound(/* node.getLocation().get() */
                node.getType(),
                value.orElse(null),
                node.getOriginalValue().orElse(null)
                );
    }

    @Override
    protected SimpleCaseExpression visitSimpleCaseExpression(SimpleCaseExpression node, C context) {

        Expression operand = processNode(node.getOperand(), context);
        List<WhenClause> whenClauses = processNodeItems(node.getWhenClauses(), context);
        Optional<Expression> defaultValue = Optional.empty();
        if (node.getDefaultValue().isPresent()) {
            defaultValue = Optional.of(processNode(node.getDefaultValue().get(), context));
        }

        return new SimpleCaseExpression(/* node.getLocation().get() */
                operand,
                whenClauses,
                defaultValue);
    }

    @Override
    protected InListExpression visitInListExpression(InListExpression node, C context)
    {
        return new InListExpression(/* node.getLocation().get() */
                processNodeItems(node.getValues(), context));
    }

    @Override
    protected NullIfExpression visitNullIfExpression(NullIfExpression node, C context)
    {
        return new NullIfExpression(/* node.getLocation().get() */
            processNode(node.getFirst(), context),
            processNode(node.getSecond(), context));
    }

    @Override
    protected IfExpression visitIfExpression(IfExpression node, C context)
    {
        return new IfExpression(/* node.getLocation().get() */
                processNode(node.getCondition(), context),
                processNode(node.getTrueValue(), context),
                node.getFalseValue().isPresent() ? processNode(node.getFalseValue().get(), context) : null
                );
    }

    @Override
    protected TryExpression visitTryExpression(TryExpression node, C context)
    {
        return new TryExpression(/* node.getLocation().get() */
                processNode(node.getInnerExpression(), context));

    }

    @Override
    protected BindExpression visitBindExpression(BindExpression node, C context)
    {
        return new BindExpression(/* node.getLocation().get() */
                processNodeItems(node.getValues(), context),
                processNode(node.getFunction(), context));
    }

    @Override
    protected ArithmeticUnaryExpression visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        return new ArithmeticUnaryExpression(/* node.getLocation().get() */
                node.getSign(),
                processNode(node.getValue(), context));
    }

    @Override
    protected NotExpression visitNotExpression(NotExpression node, C context)
    {
        return new NotExpression(/* node.getLocation().get() */
                processNode(node.getValue(), context));
    }

    @Override
    protected SearchedCaseExpression visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        List<WhenClause> whenClauses = processNodeItems(node.getWhenClauses(), context);
        Optional<Expression> defaultValue = Optional.empty();
        if(node.getDefaultValue().isPresent()){
            defaultValue = Optional.of(processNode(node.getDefaultValue().get(), context));
        }
        return new SearchedCaseExpression(/* node.getLocation().get() */
                whenClauses,
                defaultValue);
    }

    @Override
    protected LikePredicate visitLikePredicate(LikePredicate node, C context)
    {
        return new LikePredicate(/* node.getLocation().get() */
            processNode(node.getValue(), context),
            processNode(node.getPattern(), context),
            node.getEscape() != null ? processNode(node.getEscape(), context) : null);
    }

    @Override
    protected IsNotNullPredicate visitIsNotNullPredicate(IsNotNullPredicate node, C context)
    {
        return new IsNotNullPredicate(/* node.getLocation().get() */
                processNode(node.getValue(), context));
    }

    @Override
    protected IsNullPredicate visitIsNullPredicate(IsNullPredicate node, C context)
    {
        return new IsNullPredicate(/* node.getLocation().get() */
                processNode(node.getValue(), context));
    }

    @Override
    protected LogicalBinaryExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, C context)
    {
        return new LogicalBinaryExpression(/* node.getLocation().get() */
            node.getType(),
            processNode(node.getLeft(), context),
            processNode(node.getRight(), context));
    }

    @Override
    protected SubqueryExpression visitSubqueryExpression(SubqueryExpression node, C context)
    {
        return new SubqueryExpression(/* node.getLocation().get() */
                processNode(node.getQuery(), context));
    }

    @Override
    protected OrderBy visitOrderBy(OrderBy node, C context)
    {
        return new OrderBy(/* node.getLocation().get() */
                processNodeItems(node.getSortItems(), context));
    }

    @Override
    protected SortItem visitSortItem(SortItem node, C context)
    {
        return new SortItem(/* node.getLocation().get() */
                processNode(node.getSortKey(), context),
                node.getOrdering(),
                node.getNullOrdering());
    }

    @Override
    protected QuerySpecification visitQuerySpecification(QuerySpecification node, C context)
    {
        Select select = processNode(node.getSelect(), context);
        Optional<Relation> from = Optional.empty();
        Optional<Expression> where = Optional.empty();
        Optional<GroupBy> groupBy = Optional.empty();
        Optional<Expression> having = Optional.empty();
        Optional<OrderBy> orderBy = Optional.empty();

        if (node.getFrom().isPresent()) {
            from = Optional.of(processNode(node.getFrom().get(), context));
        }
        if (node.getWhere().isPresent()) {
            where = Optional.of(processNode(node.getWhere().get(), context));
        }
        if (node.getGroupBy().isPresent()) {
            groupBy = Optional.of(processNode(node.getGroupBy().get(), context));
        }
        if (node.getHaving().isPresent()) {
            having = Optional.of(processNode(node.getHaving().get(), context));
        }
        if (node.getOrderBy().isPresent()) {
            orderBy = Optional.of(processNode(node.getOrderBy().get(), context));
        }
        return new QuerySpecification(/* node.getLocation().get() */
                select, from, where,
                groupBy, having, orderBy, node.getLimit());
    }

    @Override
    protected SetOperation visitSetOperation(SetOperation node, C context)
    {
        if(node instanceof Union){
            return new Union(/* node.getLocation().get() */
                    processNodeItems(node.getRelations(), context),
                    node.isDistinct());
        } else if (node instanceof Intersect){
            return new Intersect(/* node.getLocation().get() */
                    processNodeItems(node.getRelations(), context),
                    node.isDistinct());
        } else {
            return new Except(/* node.getLocation().get() */
                    processNode(((Except) node).getLeft(), context),
                    processNode(((Except) node).getRight(), context),
                    node.isDistinct());
        }
    }

    /*
    @Override
    public Node visitUnion(Union node, C context) {
        return null;
    }

    @Override
    public Node visitIntersect(Intersect node, C context) {
        return null;
    }

    @Override
    public Node visitExcept(Except node, C context) {
        return null;
    }
    */

    @Override
    protected Values visitValues(Values node, C context)
    {
        return new Values(/* node.getLocation().get() */
                processNodeItems(node.getRows(), context));
    }

    @Override
    protected Row visitRow(Row node, C context)
    {
        return new Row(/* node.getLocation().get() */
                processNodeItems(node.getItems(), context));
    }

    @Override
    protected TableSubquery visitTableSubquery(TableSubquery node, C context)
    {
        return new TableSubquery(/* node.getLocation().get() */
                processNode(node.getQuery(), context));
    }

    @Override
    protected AliasedRelation visitAliasedRelation(AliasedRelation node, C context)
    {
        return new AliasedRelation(/* node.getLocation().get() */
                processNode(node.getRelation(), context),
                node.getAlias(),
                node.getColumnNames());
    }

    @Override
    protected SampledRelation visitSampledRelation(SampledRelation node, C context)
    {
        return new SampledRelation(/* node.getLocation().get() */
            processNode(node.getRelation(), context),
            node.getType(),
            processNode(node.getSamplePercentage(), context));
    }

    @Override
    protected Join visitJoin(Join node, C context)
    {

        Relation left = processNode(node.getLeft(), context);
        Relation right = processNode(node.getRight(), context);
        Optional<JoinCriteria> joinCriteria = Optional.empty();

        if (node.getCriteria().isPresent()) {
            JoinCriteria oldJoin = node.getCriteria().get();
            if (oldJoin instanceof JoinOn) {
                joinCriteria = Optional.of(new JoinOn(processNode(((JoinOn) oldJoin).getExpression(), context)));
            } else if (oldJoin instanceof JoinUsing) {
                joinCriteria = Optional.of(new JoinUsing(((JoinUsing) oldJoin).getColumns()));
            } else if (oldJoin instanceof NaturalJoin) {
                joinCriteria = Optional.of(oldJoin);
            }
        }

        return new Join(/* node.getLocation().get() */
                node.getType(),
                left,
                right,
                joinCriteria);
    }

    @Override
    protected Unnest visitUnnest(Unnest node, C context)
    {
        return new Unnest(/* node.getLocation().get() */
                processNodeItems(node.getExpressions(), context),
                node.isWithOrdinality());
    }

    @Override
    protected GroupBy visitGroupBy(GroupBy node, C context)
    {
        return new GroupBy(/* node.getLocation().get() */
                node.isDistinct(),
                processNodeItems(node.getGroupingElements(), context));
    }

    @Override
    protected GroupingElement visitGroupingElement(GroupingElement node, C context)
    {
        if(node instanceof SimpleGroupBy){
            return new SimpleGroupBy(/* node.getLocation().get() */
                    processNodeItems(((SimpleGroupBy) node).getColumnExpressions(), context));
        }
        // FIXME: Fix for Rollup, Cube, and GroupingSets
        return node;
    }

    @Override
    protected Insert visitInsert(Insert node, C context)
    {
        return new Insert(node.getTarget(),
                node.getColumns(),
                processNode(node.getQuery(), context));
    }

    @Override
    protected Delete visitDelete(Delete node, C context)
    {
        return new Delete(/* node.getLocation().get() */
                processNode(node.getTable(), context),
                node.getWhere().map(where -> processNode(where, context)));
    }

    @Override
    protected CreateTableAsSelect visitCreateTableAsSelect(CreateTableAsSelect node, C context)
    {
        return new CreateTableAsSelect(/* node.getLocation().get() */
                node.getName(),
                processNode(node.getQuery(), context),
                node.isNotExists(),
                processNodeItems(node.getProperties(), context),
                node.isWithData(),
                node.getColumnAliases(),
                node.getComment()
        );
    }

    @Override
    protected Property visitProperty(Property node, C context)
    {
        return new Property(/* node.getLocation().get() */
            processNode(node.getName(), context),
            processNode(node.getValue(), context));
    }

    @Override
    protected CreateView visitCreateView(CreateView node, C context)
    {
        return new CreateView(/* node.getLocation().get() */
            node.getName(),
            processNode(node.getQuery(), context),
            node.isReplace());
    }

    @Override
    protected SetSession visitSetSession(SetSession node, C context)
    {
        return new SetSession(/* node.getLocation().get() */
                node.getName(),
                processNode(node.getValue(), context)
                );
    }

    @Override
    protected AddColumn visitAddColumn(AddColumn node, C context)
    {
        return new AddColumn(/* node.getLocation().get() */
                node.getName(),
                processNode(node.getColumn(), context)
                );
    }

    @Override
    protected CreateTable visitCreateTable(CreateTable node, C context)
    {
        return new CreateTable(/* node.getLocation().get() */
                node.getName(),
                processNodeItems(node.getElements(), context),
                node.isNotExists(),
                processNodeItems(node.getProperties(), context),
                node.getComment()
                );
    }

    @Override
    protected ShowPartitions visitShowPartitions(ShowPartitions node, C context)
    {
        return new ShowPartitions(/* node.getLocation().get() */
                node.getTable(),
                node.getWhere().map(where -> processNode(where, context)),
                processNodeItems(node.getOrderBy(), context),
                node.getLimit()
                );
    }

    @Override
    protected StartTransaction visitStartTransaction(StartTransaction node, C context)
    {
        return new StartTransaction(/* node.getLocation().get() */
                processNodeItems(node.getTransactionModes(), context));
    }

    @Override
    protected Explain visitExplain(Explain node, C context)
    {
        return new Explain(/* node.getLocation().get() */
                processNode(node.getStatement(), context),
                node.isAnalyze(),
                node.isVerbose(),
                processNodeItems(node.getOptions(), context));
    }

    @Override
    protected QuantifiedComparisonExpression visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context)
    {
        return new QuantifiedComparisonExpression(/* node.getLocation().get() */
            node.getComparisonType(),
            node.getQuantifier(),
            processNode(node.getValue(), context),
            processNode(node.getSubquery(), context));
    }

    @Override
    protected ExistsPredicate visitExists(ExistsPredicate node, C context)
    {

        return new ExistsPredicate(/* node.getLocation().get() */
                processNode((Expression) node.getSubquery(), context));
    }

    @Override
    protected Lateral visitLateral(Lateral node, C context)
    {
        return new Lateral(/* node.getLocation().get() */
                processNode(node.getQuery(), context));
    }

    /*
    @Override
    public Node visitFieldReference(FieldReference node, C context) {
        return null;
    }

    @Override
    public Node visitParameter(Parameter node, C context) {
        return null;
    }

    @Override
    public Node visitPrepare(Prepare node, C context) {
        return null;
    }

    @Override
    public Node visitDeallocate(Deallocate node, C context) {
        return null;
    }

    @Override
    public Node visitExecute(Execute node, C context) {
        return null;
    }

    @Override
    public Node visitDescribeOutput(DescribeOutput node, C context) {
        return null;
    }

    @Override
    public Node visitDescribeInput(DescribeInput node, C context) {
        return null;
    }

    @Override
    public Node visitCreateSchema(CreateSchema node, C context) {
        return null;
    }

    @Override
    public Node visitDropSchema(DropSchema node, C context) {
        return null;
    }

    @Override
    public Node visitRenameSchema(RenameSchema node, C context) {
        return null;
    }

    @Override
    public Node visitDropTable(DropTable node, C context) {
        return null;
    }

    @Override
    public Node visitRenameTable(RenameTable node, C context) {
        return null;
    }

    @Override
    public Node visitRenameColumn(RenameColumn node, C context) {
        return null;
    }

    @Override
    public Node visitDropColumn(DropColumn node, C context) {
        return null;
    }

    @Override
    public Node visitDropView(DropView node, C context) {
        return null;
    }

    @Override
    public Node visitResetSession(ResetSession node, C context) {
        return null;
    }

    @Override
    public Node visitExplainOption(ExplainOption node, C context) {
        return null;
    }

    @Override
    public Node visitShowCreate(ShowCreate node, C context) {
        return null;
    }

    @Override
    public Node visitShowFunctions(ShowFunctions node, C context) {
        return null;
    }

    @Override
    public Node visitUse(Use node, C context) {
        return null;
    }

    @Override
    public Node visitShowSession(ShowSession node, C context) {
        return null;
    }

    @Override
    public Node visitGrant(Grant node, C context) {
        return null;
    }

    @Override
    public Node visitRevoke(Revoke node, C context) {
        return null;
    }

    @Override
    public Node visitShowGrants(ShowGrants node, C context) {
        return null;
    }

    @Override
    public Node visitTransactionMode(TransactionMode node, C context) {
        return null;
    }

    @Override
    public Node visitIsolationLevel(Isolation node, C context) {
        return null;
    }

    @Override
    public Node visitTransactionAccessMode(TransactionAccessMode node, C context) {
        return null;
    }

    @Override
    public Node visitCommit(Commit node, C context) {
        return null;
    }

    @Override
    public Node visitRollback(Rollback node, C context) {
        return null;
    }

    @Override
    public Node visitShowTables(ShowTables node, C context) {
        return null;
    }

    @Override
    public Node visitShowSchemas(ShowSchemas node, C context) {
        return null;
    }

    @Override
    public Node visitShowCatalogs(ShowCatalogs node, C context) {
        return null;
    }
*/
    @Override
    public Node visitShowColumns(ShowColumns node, C context) {
        QualifiedName table = node.getTable();
        return new ShowColumns(table);
    }

    @Override
    protected Node visitBooleanLiteral(BooleanLiteral node, C context) {
        String value = String.valueOf(node.getValue());
        return new BooleanLiteral(value);
    }

    /*
    @Override
    public Node visitShowStats(ShowStats node, C context) {
        return null;
    }

    */
}
