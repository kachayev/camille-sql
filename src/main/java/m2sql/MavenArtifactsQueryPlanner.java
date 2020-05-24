package m2sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.ValidationException;

public class MavenArtifactsQueryPlanner {

    private final Planner planner;

    public MavenArtifactsQueryPlanner(SchemaPlus schema) {
        final List<RelTraitDef> traitDefs = new ArrayList<>();

        traitDefs.add(ConventionTraitDef.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder().setLex(Lex.MYSQL_ANSI).build()).defaultSchema(schema)
                .traitDefs(traitDefs).context(Contexts.EMPTY_CONTEXT).ruleSets(RuleSets.ofList()).costFactory(null)
                .typeSystem(RelDataTypeSystem.DEFAULT).build();

        this.planner = Frameworks.getPlanner(frameworkConfig);
    }

    public RelNode getLogicalPlan(String query) throws ValidationException, RelConversionException {
        SqlNode sqlNode;

        try {
            sqlNode = planner.parse(query);
        } catch (SqlParseException e) {
            throw new RuntimeException("Query parsing error.", e);
        }

        SqlNode validateSqlNode = planner.validate(sqlNode);
        return planner.rel(validateSqlNode).project();
    }

    public static void main(String[] args) {
        final SqlParser.Config parserConfig = SqlParser.configBuilder()
            .setLex(Lex.MYSQL_ANSI)
            .setParserFactory(SqlBabelParserImpl.FACTORY)
            .build();

        final FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(parserConfig)
                .context(Contexts.EMPTY_CONTEXT)
                .ruleSets(RuleSets.ofList())
                .costFactory(null)
                .typeSystem(RelDataTypeSystem.DEFAULT)
                .build();

        final Planner planner = Frameworks.getPlanner(frameworkConfig);

        try {
            final SqlNode node = planner.parse("SELECT pg_catalog.array_to_string(d.datacl, '\"\n\"') AS \"Access privileges\" FROM pg_catalog.pg_database d ORDER BY 1");
            System.out.println(node.toSqlString(AnsiSqlDialect.DEFAULT));
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }

}