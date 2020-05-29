package m2sql;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class MavenArtifactsDatabase {

    private static final String JDBC_CALCITE_SCHEMA = "jdbc:calcite:";

    private final MavenArtifactsResolver resolver;
 
    private final Table artifactsTable;

    private final Table versionsTable;

    public MavenArtifactsDatabase(String baseFolder) {
        this.resolver = new MavenArtifactsResolver(baseFolder);
        this.artifactsTable = new MavenArtifactsTable(this.resolver);
        this.versionsTable = new MavenArtifactVersionsTable(this.resolver);
    }

    public Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", Lex.MYSQL_ANSI.toString());
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

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        final String userFolder = System.getProperty("user.home");
        final String m2Folder = Paths.get(userFolder, ".m2/repository/").toString();
        MavenArtifactsDatabase db = new MavenArtifactsDatabase(m2Folder);

        try (
            final Connection conn = db.createConnection();
            final Statement statement = conn.createStatement()
        ) {
            try (final ResultSet resultSet = statement.executeQuery("select group_id from artifacts where group_id LIKE 'nrepl.%'")) {
                printResultSet(resultSet);
            }

            try (final ResultSet resultSet = statement.executeQuery("select uid, filesize from versions")) {
                printResultSet(resultSet);
            }

            try (final ResultSet resultSet = statement.executeQuery("select * from versions where filesize > 10000 limit 5")) {
                printResultSet(resultSet);
            }

            try (final ResultSet resultSet = statement.executeQuery("select group_id, count(*) as n_files from artifacts left join versions on artifacts.uid=versions.uid group by group_id order by n_files desc")) {
                printResultSet(resultSet);
            }

            try (final ResultSet resultSet = statement.executeQuery("select group_id, sum(filesize) as total_size from artifacts left join versions on artifacts.uid=versions.uid group by group_id order by total_size desc")) {
                printResultSet(resultSet);
            }
        }
    }

}