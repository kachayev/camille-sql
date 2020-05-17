package m2sql;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

public class MavenArtifactsTable implements ProjectableFilterableTable {

    private final MavenArtifactsResolver resolver;

    protected final RelProtoDataType protoRowType = new RelProtoDataType() {
        public RelDataType apply(RelDataTypeFactory typeFactory) {
            return new RelDataTypeFactory.Builder(typeFactory)
                .add("uid", SqlTypeName.BIGINT)
                .add("group_id", SqlTypeName.VARCHAR, 1023)
                .add("artifact_id", SqlTypeName.VARCHAR, 255)
                .build();
        }
    };

    public MavenArtifactsTable(MavenArtifactsResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent,
            CalciteConnectionConfig config) {
        return false;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return protoRowType.apply(typeFactory);
    }

    @Override
    public TableType getJdbcTableType() {
        return TableType.TABLE;
    }

    // xxx(okachaiev): predicate push-down for subfolders
    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        try {
            return Linq4j
                    .asEnumerable(resolver.findAll()
                        .map(artifact -> artifact.toRow(projects))
                        .collect(Collectors.toList()));
        } catch (IOException e) {
            // xxx(okachaiev): log error
            return Linq4j.emptyEnumerable();
        }
    }

}