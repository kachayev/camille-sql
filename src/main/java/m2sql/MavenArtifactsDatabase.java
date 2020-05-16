package m2sql;

import java.io.IOException;
import java.nio.file.Paths;
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

public class MavenArtifactsDatabase {

    private static final String JDBC_CALCITE_SCHEMA = "jdbc:calcite:";

    private final MavenArtifactsResolver resolver;
 
    private final Table artifactsTable = new ScannableTable() {
        protected final RelProtoDataType protoRowType = new RelProtoDataType() {
            public RelDataType apply(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                    .add("uid", SqlTypeName.BIGINT)
                    .add("group_id", SqlTypeName.VARCHAR, 1023)
                    .add("artifact_id", SqlTypeName.VARCHAR, 255)
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

        // xxx(okachaiev): predicate push-down for subfolders
        @Override
        public Enumerable<Object[]> scan(DataContext root) {
			try {
				return Linq4j.asEnumerable(
                    resolver.findAll()
                        .map(artifact -> artifact.toRow())
                        .collect(Collectors.toList()));
			} catch (IOException e) {
                // xxx(okachaiev): log error
				return Linq4j.emptyEnumerable();
			}
        }
    };

    private final Table versionsTable = new ScannableTable() {
        protected final RelProtoDataType protoRowType = new RelProtoDataType() {
            public RelDataType apply(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                    .add("uid", SqlTypeName.BIGINT)
                    .add("version", SqlTypeName.VARCHAR, 255)
                    .add("filesize", SqlTypeName.BIGINT)
                    .add("last_modified", SqlTypeName.DATE)
                    .add("sha1", SqlTypeName.VARCHAR, 40)
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
                    resolver.findAllVersions()
                        .map(version -> version.toRow())
                        .collect(Collectors.toList()));
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
        Connection connection = DriverManager.getConnection(JDBC_CALCITE_SCHEMA, info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("artifacts", artifactsTable);
        rootSchema.add("versions", versionsTable);
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

        final String userFolder = System.getProperty("user.home");
        final String m2Folder = Paths.get(userFolder, ".m2/repository/").toString();
        MavenArtifactsDatabase db = new MavenArtifactsDatabase(m2Folder);

        try (final Connection conn = db.createConnection()) {
            try (final Statement statement = conn.createStatement()) {
                try (final ResultSet resultSet = statement.executeQuery("select * from artifacts")) {
                    printResultSet(resultSet);
                    resultSet.close();
                }

                try (final ResultSet resultSet = statement.executeQuery("select * from versions")) {
                    printResultSet(resultSet);
                    resultSet.close();
                }

                try (final ResultSet resultSet = statement.executeQuery("select group_id, count(*) as n_files from artifacts left join versions on artifacts.uid=versions.uid group by group_id order by n_files desc")) {
                    printResultSet(resultSet);
                    resultSet.close();
                }

                try (final ResultSet resultSet = statement.executeQuery("select group_id, sum(filesize) as total_size from artifacts left join versions on artifacts.uid=versions.uid group by group_id order by total_size desc")) {
                    printResultSet(resultSet);
                    resultSet.close();
                }
            }
        }
    }

}