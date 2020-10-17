package m2sql;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;

public class MavenArtifactsQueryPlanner {

    private final Planner planner;

    private final AnsiSqlDialect sqlDialect;

    public MavenArtifactsQueryPlanner(SchemaPlus schema) {
        final List<RelTraitDef> traitDefs = new ArrayList<>();

        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);

        sqlDialect = new AnsiSqlDialect(SqlDialect.EMPTY_CONTEXT.withDatabaseProduct(SqlDialect.DatabaseProduct.UNKNOWN)
                .withIdentifierQuoteString("'"));

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL_ANSI).build()).defaultSchema(schema)
                .traitDefs(traitDefs).context(Contexts.EMPTY_CONTEXT).ruleSets(RuleSets.ofList()).costFactory(null)
                .typeSystem(RelDataTypeSystem.DEFAULT).build();

        this.planner = Frameworks.getPlanner(frameworkConfig);
    }

    public RelRoot getLogicalPlan(String query) throws ValidationException, RelConversionException, SqlParseException {
        final SqlNode sqlNode = this.planner.parse(query);
        final SqlNode validateSqlNode = this.planner.validate(sqlNode);
        return planner.rel(validateSqlNode);
    }

    public SqlString parse(String query) throws SqlParseException {
        final SqlNode node = this.planner.parse(query);
        return node.toSqlString(this.sqlDialect);
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        final String userFolder = System.getProperty("user.home");
        final String m2Folder = Paths.get(userFolder, ".m2/repository/").toString();
        MavenArtifactsDatabase db = new MavenArtifactsDatabase(m2Folder);

        try (final Connection conn = db.createConnection()) {
            final CalciteConnection calciteConnection = (CalciteConnection) conn;
            final MavenArtifactsQueryPlanner planner = new MavenArtifactsQueryPlanner(calciteConnection.getRootSchema());

            // final SqlString node = planner.parse("select group_id, count(*) as n_files from versions group by group_id order by n_files desc");
            // System.out.println(node);

            // final RelRoot root = planner.getLogicalPlan("select group_id, name from artifacts");
            // final RelRoot root = planner.getLogicalPlan("select group_id, name from artifacts where group_id = 'nrepl' order by artifact_id limit 20");
            final RelRoot root = planner.getLogicalPlan("select group_id, count(*) as n_files from artifacts left join versions on artifacts.uid=versions.uid group by group_id order by n_files desc");
            System.out.println(RelOptUtil.toString(root.project()));
            System.out.println(root);
        } catch (SqlParseException | RelConversionException | ValidationException e) {
            e.printStackTrace();
        }
    }

}