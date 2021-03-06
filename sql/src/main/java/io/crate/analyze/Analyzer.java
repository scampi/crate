/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.auth.user.UserManager;
import io.crate.execution.ddl.RepositoryService;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.sql.tree.AlterBlobTable;
import io.crate.sql.tree.AlterClusterRerouteRetryFailed;
import io.crate.sql.tree.AlterTable;
import io.crate.sql.tree.AlterTableAddColumn;
import io.crate.sql.tree.AlterTableOpenClose;
import io.crate.sql.tree.AlterTableRename;
import io.crate.sql.tree.AlterTableReroute;
import io.crate.sql.tree.AlterUser;
import io.crate.sql.tree.AnalyzeStatement;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BeginStatement;
import io.crate.sql.tree.CommitStatement;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.CopyTo;
import io.crate.sql.tree.CreateAnalyzer;
import io.crate.sql.tree.CreateBlobTable;
import io.crate.sql.tree.CreateFunction;
import io.crate.sql.tree.CreateRepository;
import io.crate.sql.tree.CreateSnapshot;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CreateUser;
import io.crate.sql.tree.CreateView;
import io.crate.sql.tree.DeallocateStatement;
import io.crate.sql.tree.DecommissionNodeStatement;
import io.crate.sql.tree.Delete;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.DropAnalyzer;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropFunction;
import io.crate.sql.tree.DropRepository;
import io.crate.sql.tree.DropSnapshot;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.DropUser;
import io.crate.sql.tree.DropView;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GCDanglingArtifacts;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.InsertFromSubquery;
import io.crate.sql.tree.InsertFromValues;
import io.crate.sql.tree.KillStatement;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.OptimizeStatement;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.RefreshStatement;
import io.crate.sql.tree.ResetStatement;
import io.crate.sql.tree.RestoreSnapshot;
import io.crate.sql.tree.RevokePrivilege;
import io.crate.sql.tree.SetStatement;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowCreateTable;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowSessionParameter;
import io.crate.sql.tree.ShowTables;
import io.crate.sql.tree.ShowTransaction;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.SwapTable;
import io.crate.sql.tree.Update;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.analysis.AnalysisRegistry;

import java.util.Locale;

@Singleton
public class Analyzer {

    private final AnalyzerDispatcher dispatcher = new AnalyzerDispatcher();

    private final RelationAnalyzer relationAnalyzer;
    private final DropTableAnalyzer dropTableAnalyzer;
    private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
    private final ExplainStatementAnalyzer explainStatementAnalyzer;
    private final ShowStatementAnalyzer showStatementAnalyzer;
    private final CreateBlobTableAnalyzer createBlobTableAnalyzer;
    private final CreateAnalyzerStatementAnalyzer createAnalyzerStatementAnalyzer;
    private final DropAnalyzerStatementAnalyzer dropAnalyzerStatementAnalyzer;
    private final UserManager userManager;
    private final RefreshTableAnalyzer refreshTableAnalyzer;
    private final OptimizeTableAnalyzer optimizeTableAnalyzer;
    private final AlterTableAnalyzer alterTableAnalyzer;
    private final AlterTableAddColumnAnalyzer alterTableAddColumnAnalyzer;
    private final InsertFromValuesAnalyzer insertFromValuesAnalyzer;
    private final InsertFromSubQueryAnalyzer insertFromSubQueryAnalyzer;
    private final CopyAnalyzer copyAnalyzer;
    private final UpdateAnalyzer updateAnalyzer;
    private final DeleteAnalyzer deleteAnalyzer;
    private final DropRepositoryAnalyzer dropRepositoryAnalyzer;
    private final CreateRepositoryAnalyzer createRepositoryAnalyzer;
    private final DropSnapshotAnalyzer dropSnapshotAnalyzer;
    private final CreateSnapshotAnalyzer createSnapshotAnalyzer;
    private final RestoreSnapshotAnalyzer restoreSnapshotAnalyzer;
    private final UnboundAnalyzer unboundAnalyzer;
    private final CreateFunctionAnalyzer createFunctionAnalyzer;
    private final DropFunctionAnalyzer dropFunctionAnalyzer;
    private final PrivilegesAnalyzer privilegesAnalyzer;
    private final AlterTableRerouteAnalyzer alterTableRerouteAnalyzer;
    private final UserAnalyzer userAnalyzer;
    private final ViewAnalyzer viewAnalyzer;
    private final SwapTableAnalyzer swapTableAnalyzer;
    private final DecommissionNodeAnalyzer decommissionNodeAnalyzer;
    private final KillAnalyzer killAnalyzer;
    private final SetStatementAnalyzer setStatementAnalyzer;
    private final ResetStatementAnalyzer resetStatementAnalyzer;

    /**
     * @param relationAnalyzer is injected because we also need to inject it in
     *                         {@link io.crate.metadata.view.InternalViewInfoFactory} and we want to keep only a single
     *                         instance of the class
     */
    @Inject
    public Analyzer(Schemas schemas,
                    Functions functions,
                    RelationAnalyzer relationAnalyzer,
                    ClusterService clusterService,
                    AnalysisRegistry analysisRegistry,
                    RepositoryService repositoryService,
                    UserManager userManager) {
        this.relationAnalyzer = relationAnalyzer;
        this.dropTableAnalyzer = new DropTableAnalyzer(schemas);
        this.userManager = userManager;
        this.createTableStatementAnalyzer = new CreateTableStatementAnalyzer(functions);
        this.alterTableAnalyzer = new AlterTableAnalyzer(schemas, functions);
        this.alterTableAddColumnAnalyzer = new AlterTableAddColumnAnalyzer(schemas, functions);
        this.swapTableAnalyzer = new SwapTableAnalyzer(functions, schemas);
        this.viewAnalyzer = new ViewAnalyzer(relationAnalyzer, schemas);
        this.explainStatementAnalyzer = new ExplainStatementAnalyzer(this);
        this.showStatementAnalyzer = new ShowStatementAnalyzer(this, schemas);
        this.updateAnalyzer = new UpdateAnalyzer(functions, relationAnalyzer);
        this.deleteAnalyzer = new DeleteAnalyzer(functions, relationAnalyzer);
        this.insertFromValuesAnalyzer = new InsertFromValuesAnalyzer(functions, schemas);
        this.insertFromSubQueryAnalyzer = new InsertFromSubQueryAnalyzer(functions, schemas, relationAnalyzer);
        this.optimizeTableAnalyzer = new OptimizeTableAnalyzer(schemas, functions);
        this.createRepositoryAnalyzer = new CreateRepositoryAnalyzer(repositoryService, functions);
        this.dropRepositoryAnalyzer = new DropRepositoryAnalyzer(repositoryService);
        this.createSnapshotAnalyzer = new CreateSnapshotAnalyzer(repositoryService, functions);
        this.dropSnapshotAnalyzer = new DropSnapshotAnalyzer(repositoryService);
        this.userAnalyzer = new UserAnalyzer(functions);
        this.createBlobTableAnalyzer = new CreateBlobTableAnalyzer(schemas, functions);
        this.createFunctionAnalyzer = new CreateFunctionAnalyzer(functions);
        this.dropFunctionAnalyzer = new DropFunctionAnalyzer();
        this.refreshTableAnalyzer = new RefreshTableAnalyzer(functions, schemas);
        this.restoreSnapshotAnalyzer = new RestoreSnapshotAnalyzer(repositoryService, functions);
        FulltextAnalyzerResolver fulltextAnalyzerResolver =
            new FulltextAnalyzerResolver(clusterService, analysisRegistry);
        this.createAnalyzerStatementAnalyzer = new CreateAnalyzerStatementAnalyzer(fulltextAnalyzerResolver, functions);
        this.dropAnalyzerStatementAnalyzer = new DropAnalyzerStatementAnalyzer(fulltextAnalyzerResolver);
        this.decommissionNodeAnalyzer = new DecommissionNodeAnalyzer(functions);
        this.killAnalyzer = new KillAnalyzer(functions);
        this.alterTableRerouteAnalyzer = new AlterTableRerouteAnalyzer(functions, schemas);
        this.privilegesAnalyzer = new PrivilegesAnalyzer(userManager.isEnabled(), schemas);
        this.copyAnalyzer = new CopyAnalyzer(schemas, functions);
        this.setStatementAnalyzer = new SetStatementAnalyzer(functions);
        this.resetStatementAnalyzer = new ResetStatementAnalyzer(functions);
        this.unboundAnalyzer = new UnboundAnalyzer(
            relationAnalyzer,
            showStatementAnalyzer,
            deleteAnalyzer,
            updateAnalyzer,
            insertFromValuesAnalyzer,
            insertFromSubQueryAnalyzer,
            explainStatementAnalyzer,
            createTableStatementAnalyzer,
            alterTableAnalyzer,
            alterTableAddColumnAnalyzer,
            optimizeTableAnalyzer,
            createRepositoryAnalyzer,
            dropRepositoryAnalyzer,
            createSnapshotAnalyzer,
            dropSnapshotAnalyzer,
            userAnalyzer,
            createBlobTableAnalyzer,
            createFunctionAnalyzer,
            dropFunctionAnalyzer,
            dropTableAnalyzer,
            refreshTableAnalyzer,
            restoreSnapshotAnalyzer,
            createAnalyzerStatementAnalyzer,
            dropAnalyzerStatementAnalyzer,
            decommissionNodeAnalyzer,
            killAnalyzer,
            alterTableRerouteAnalyzer,
            privilegesAnalyzer,
            copyAnalyzer,
            viewAnalyzer,
            swapTableAnalyzer,
            setStatementAnalyzer,
            resetStatementAnalyzer
        );
    }

    public Analysis boundAnalyze(Statement statement, CoordinatorTxnCtx coordinatorTxnCtx, ParameterContext parameterContext) {
        Analysis analysis = new Analysis(coordinatorTxnCtx, parameterContext, ParamTypeHints.EMPTY);
        AnalyzedStatement analyzedStatement = analyzedStatement(statement, analysis);
        SessionContext sessionContext = coordinatorTxnCtx.sessionContext();
        userManager.getAccessControl(sessionContext).ensureMayExecute(analyzedStatement);
        analysis.analyzedStatement(analyzedStatement);
        return analysis;
    }

    public AnalyzedStatement unboundAnalyze(Statement statement, SessionContext sessionContext, ParamTypeHints paramTypeHints) {
        return unboundAnalyzer.analyze(statement, sessionContext, paramTypeHints);
    }

    AnalyzedStatement analyzedStatement(Statement statement, Analysis analysis) {
        AnalyzedStatement analyzedStatement = statement.accept(dispatcher, analysis);
        assert analyzedStatement != null : "analyzed statement must not be null";
        return analyzedStatement;
    }

    private class AnalyzerDispatcher extends AstVisitor<AnalyzedStatement, Analysis> {

        @Override
        protected AnalyzedStatement visitQuery(Query node, Analysis analysis) {
            AnalyzedRelation relation = relationAnalyzer.analyze(
                node,
                analysis.transactionContext(),
                analysis.parameterContext());
            analysis.rootRelation(relation);
            return relation;
        }

        @Override
        public AnalyzedStatement visitAnalyze(AnalyzeStatement analyzeStatement, Analysis context) {
            return new AnalyzedAnalyze();
        }

        @Override
        public AnalyzedStatement visitDelete(Delete node, Analysis context) {
            return deleteAnalyzer.analyze(node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitInsertFromValues(InsertFromValues node, Analysis context) {
            return insertFromValuesAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitInsertFromSubquery(InsertFromSubquery node, Analysis context) {
            return insertFromSubQueryAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitUpdate(Update node, Analysis context) {
            return updateAnalyzer.analyze(node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCopyFrom(CopyFrom<?> node, Analysis context) {
            return copyAnalyzer.analyzeCopyFrom(
                (CopyFrom<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCopyTo(CopyTo<?> node, Analysis context) {
            return copyAnalyzer.analyzeCopyTo(
                (CopyTo<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDropTable(DropTable<?> node, Analysis context) {
            return dropTableAnalyzer.analyze(node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitCreateTable(CreateTable<?> node, Analysis analysis) {
            return createTableStatementAnalyzer.analyze((CreateTable<Expression>) node, analysis.paramTypeHints(), analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitShowCreateTable(ShowCreateTable node, Analysis analysis) {
            return showStatementAnalyzer.analyzeShowCreateTable(node.table(), analysis);
        }

        @Override
        public AnalyzedStatement visitShowSchemas(ShowSchemas node, Analysis analysis) {
            return showStatementAnalyzer.analyze(node, analysis);
        }

        @Override
        public AnalyzedStatement visitShowTransaction(ShowTransaction showTransaction, Analysis context) {
            return showStatementAnalyzer.analyzeShowTransaction(context);
        }

        @Override
        public AnalyzedStatement visitShowTables(ShowTables node, Analysis analysis) {
            return showStatementAnalyzer.analyze(node, analysis);
        }

        @Override
        protected AnalyzedStatement visitShowColumns(ShowColumns node, Analysis context) {
            return showStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitShowSessionParameter(ShowSessionParameter node, Analysis context) {
            return showStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitCreateAnalyzer(CreateAnalyzer<?> node, Analysis context) {
            return createAnalyzerStatementAnalyzer.analyze(
                (CreateAnalyzer<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDropAnalyzer(DropAnalyzer node, Analysis context) {
            return dropAnalyzerStatementAnalyzer.analyze(node.name());
        }

        @Override
        public AnalyzedStatement visitCreateBlobTable(CreateBlobTable<?> node, Analysis context) {
            return createBlobTableAnalyzer.analyze(
                (CreateBlobTable<Expression>) node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDropBlobTable(DropBlobTable<?> node, Analysis context) {
            return dropTableAnalyzer.analyze(node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitAlterBlobTable(AlterBlobTable<?> node, Analysis context) {
            return alterTableAnalyzer.analyze(
                (AlterBlobTable<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitRefreshStatement(RefreshStatement<?> node, Analysis context) {
            return refreshTableAnalyzer.analyze(
                (RefreshStatement<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext()
            );
        }

        @Override
        public AnalyzedStatement visitAlterTable(AlterTable<?> node, Analysis context) {
            return alterTableAnalyzer.analyze(
                (AlterTable<Expression>) node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterClusterRerouteRetryFailed(AlterClusterRerouteRetryFailed node, Analysis context) {
            return new AnalyzedRerouteRetryFailed();
        }

        @Override
        public AnalyzedStatement visitAlterTableRename(AlterTableRename<?> node, Analysis context) {
            return alterTableAnalyzer.analyze((AlterTableRename<Expression>) node, context.sessionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableAddColumnStatement(AlterTableAddColumn<?> node, Analysis analysis) {
            return alterTableAddColumnAnalyzer.analyze(
                (AlterTableAddColumn<Expression>) node,
                analysis.paramTypeHints(),
                analysis.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableOpenClose(AlterTableOpenClose<?> node, Analysis context) {
            return alterTableAnalyzer.analyze((AlterTableOpenClose<Expression>) node,
                                              context.paramTypeHints(),
                                              context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitAlterTableReroute(AlterTableReroute<?> node, Analysis context) {
            return alterTableRerouteAnalyzer.analyze(
                (AlterTableReroute<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitSetStatement(SetStatement<?> node, Analysis context) {
            return setStatementAnalyzer.analyze((SetStatement<Expression>)node,
                                                context.paramTypeHints(),
                                                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitResetStatement(ResetStatement<?> node, Analysis context) {
            return resetStatementAnalyzer.analyze((ResetStatement<Expression>) node,
                                                context.paramTypeHints(),
                                                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitKillStatement(KillStatement<?> node, Analysis context) {
            return killAnalyzer.analyze(
                (KillStatement<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDeallocateStatement(DeallocateStatement node, Analysis context) {
            return DeallocateAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitDropRepository(DropRepository node, Analysis context) {
            return dropRepositoryAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitCreateRepository(CreateRepository node, Analysis context) {
            return createRepositoryAnalyzer.analyze(
                (CreateRepository<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitCreateFunction(CreateFunction<?> node, Analysis context) {
            return createFunctionAnalyzer.analyze(
                (CreateFunction<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext(),
                context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitDropFunction(DropFunction node, Analysis context) {
            return dropFunctionAnalyzer.analyze(node, context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitCreateUser(CreateUser<?> node, Analysis context) {
            return userAnalyzer.analyze(
                (CreateUser<Expression>) node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDropUser(DropUser node, Analysis context) {
            return new AnalyzedDropUser(
                node.name(),
                node.ifExists()
            );
        }

        @Override
        public AnalyzedStatement visitAlterUser(AlterUser<?> node, Analysis context) {
            return userAnalyzer.analyze(
                (AlterUser<Expression>) node, context.paramTypeHints(), context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitGrantPrivilege(GrantPrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeGrant(
                node,
                context.sessionContext().user(),
                context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitDenyPrivilege(DenyPrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeDeny(
                node,
                context.sessionContext().user(),
                context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitRevokePrivilege(RevokePrivilege node, Analysis context) {
            return privilegesAnalyzer.analyzeRevoke(
                node,
                context.sessionContext().user(),
                context.sessionContext().searchPath());
        }

        @Override
        public AnalyzedStatement visitOptimizeStatement(OptimizeStatement<?> node, Analysis context) {
            return optimizeTableAnalyzer.analyze(
                (OptimizeStatement<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitDropSnapshot(DropSnapshot node, Analysis context) {
            return dropSnapshotAnalyzer.analyze(node);
        }

        @Override
        public AnalyzedStatement visitCreateSnapshot(CreateSnapshot<?> node, Analysis context) {
            return createSnapshotAnalyzer.analyze(
                (CreateSnapshot<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitRestoreSnapshot(RestoreSnapshot node, Analysis context) {
            return restoreSnapshotAnalyzer.analyze(
                (RestoreSnapshot<Expression>) node,
                context.paramTypeHints(),
                context.transactionContext());
        }

        @Override
        protected AnalyzedStatement visitExplain(Explain node, Analysis context) {
            return explainStatementAnalyzer.analyze(node, context);
        }

        @Override
        public AnalyzedStatement visitBegin(BeginStatement node, Analysis context) {
            return new AnalyzedBegin();
        }

        @Override
        public AnalyzedStatement visitCommit(CommitStatement node, Analysis context) {
            return new AnalyzedCommit();
        }

        @Override
        protected AnalyzedStatement visitNode(Node node, Analysis context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "cannot analyze statement: '%s'", node));
        }

        @Override
        public AnalyzedStatement visitCreateView(CreateView node, Analysis context) {
            return viewAnalyzer.analyze(node, context.transactionContext());
        }

        @Override
        public AnalyzedStatement visitSwapTable(SwapTable<?> swapTable, Analysis analysis) {
            return swapTableAnalyzer.analyze(
                (SwapTable<Expression>) swapTable,
                analysis.transactionContext(),
                analysis.paramTypeHints()
            );
        }

        @Override
        public AnalyzedStatement visitGCDanglingArtifacts(GCDanglingArtifacts gcDanglingArtifacts, Analysis context) {
            return AnalyzedGCDanglingArtifacts.INSTANCE;
        }

        @Override
        public AnalyzedStatement visitAlterClusterDecommissionNode(DecommissionNodeStatement<?> node,
                                                                   Analysis context) {
            return decommissionNodeAnalyzer.analyze(
                (DecommissionNodeStatement<Expression>) node,
                context.transactionContext(),
                context.paramTypeHints());
        }

        @Override
        public AnalyzedStatement visitDropView(DropView node, Analysis context) {
            return viewAnalyzer.analyze(node, context.transactionContext());
        }
    }
}
