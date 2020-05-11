package m2sql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import m2sql.MavenArtifactsResolver.Artifact;

public class MavenArtifactsDatabase {

    private final MavenArtifactsResolver resolver;

    private final Table artifactsTable = new ScannableTable() {
        protected final RelProtoDataType protoRowType = new RelProtoDataType() {
            public RelDataType apply(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                    .add("uid", SqlTypeName.BIGINT)
                    .add("groupId", SqlTypeName.VARCHAR, 1023)
                    .add("artifactId", SqlTypeName.VARCHAR, 255)
                    .build();
            }
        };

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

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
			try {
				return Linq4j.asEnumerable(
                    resolver.findAll().map(artifact -> artifact.toRow()).collect(Collectors.toList()));
			} catch (IOException e) {
                // xxx(okachaiev): log error
				return Linq4j.emptyEnumerable();
			}
        }
    };

    public MavenArtifactsDatabase(String baseFolder) {
        this.resolver = new MavenArtifactsResolver(baseFolder);
    }

    public Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", Lex.MYSQL.toString());
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("artifacts", artifactsTable);
        return calciteConnection;
    }

    private static final void printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int numColumns = metadata.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; i <= numColumns; i++) {
                if (i > 1) System.out.print(",  ");
                String columnValue = resultSet.getString(i);
                System.out.print(columnValue + " " + metadata.getColumnName(i));
            }
            System.out.println("");
        }        
    } 

    public static void main(String[] args)
            throws ValidationException, RelConversionException, ClassNotFoundException, SQLException {

        final String baseFolder = "/Users/kachayev/.m2/repository/";
        MavenArtifactsDatabase db = new MavenArtifactsDatabase(baseFolder);

        final Connection conn = db.createConnection();
        final Statement statement = conn.createStatement();
        final ResultSet resultSet = statement.executeQuery("select * from artifacts");
        printResultSet(resultSet);
        resultSet.close();
        statement.close();
        conn.close();
    }

}