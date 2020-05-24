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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

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

    private boolean isColumnLiteral(final List<RexNode> operands) {
        final RexNode columnInput = operands.get(0);
        return columnInput instanceof RexInputRef
            && ((RexInputRef) columnInput).getName().equals("$1");
    }

    // trying to find filters in for of "where group_id = X"
    // note, that compiler won't push filter if more
    // compilated logic should be applied between leafs,
    // like "where group_id = X or group_id = Y"
    // or, even worse, "where group_id = X and group_id = Y"
    //
    // a use case that's harder to capture (but still possible)
    // "where group_id LIKE 'nrepl.%'"
    private String pushDownFolder(List<RexNode> filters) {
        for (final RexNode filter: filters) {
            final RexCall call = (RexCall) filter;
            final List<RexNode> operands = call.getOperands();
            if (!isColumnLiteral(operands)) {
                // hardly we can do anything here
                continue;
            }
            if (filter.isA(SqlKind.EQUALS)) {
                final RexNode columnLiteralValue = operands.get(1);
                if (columnLiteralValue instanceof RexLiteral) {
                    final NlsString value = (NlsString) ((RexLiteral) columnLiteralValue).getValue();
                    return value.getValue();
                }
            } else if (filter.isA(SqlKind.LIKE)) {
                final RexNode columnLiteralValue = operands.get(1);
                if (columnLiteralValue instanceof RexLiteral) {
                    final NlsString value = (NlsString) ((RexLiteral) columnLiteralValue).getValue();
                    final String stringValue = value.getValue();
                    if (stringValue.endsWith(".%")) {
                        final String likePredicate = stringValue.substring(0, stringValue.length()-2);
                        // this means that we're looking for something like "nrepl.%"
                        // and not "%.apache.%"
                        if (!likePredicate.contains("%")) {
                            return likePredicate;
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        final String folder = pushDownFolder(filters);

        try {
            return Linq4j
                    .asEnumerable(resolver.findAll(folder)
                        .map(artifact -> artifact.toRow(projects))
                        .collect(Collectors.toList()));
        } catch (IOException e) {
            // xxx(okachaiev): log error
            return Linq4j.emptyEnumerable();
        }
    }

}