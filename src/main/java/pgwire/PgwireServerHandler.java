package pgwire;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.Meta;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import m2sql.MavenArtifactsDatabase;

public class PgwireServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final String ERROR_CODE_DATA_EXCEPTION = "22000";

    private static final String ERROR_CODE_INVALID_SQL_STATEMENT = "26000";

    private static final String ERROR_MESSAGE_NON_SELECT_STATEMENT = "Only SELECT statemenet are supported";

    private static final PgwireCommandComplete SELECT_COMMAND_COMPLETE_MESSAGE =
        new PgwireCommandComplete("SELECT");

    private static final PgwireReadyForQuery READY_FOR_QUERY_MESSAGE = new PgwireReadyForQuery();

    private final MavenArtifactsDatabase db;

    private Connection conn;

    private final String pgVersion;

    public PgwireServerHandler(MavenArtifactsDatabase db, String pgVersion) {
        super();
        this.db = db;
        this.pgVersion = pgVersion;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.conn = db.createConnection();
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        this.conn.close();
        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof PgwireStartupMessage) {
            // xxx(okachaiev): log
            System.out.println(msg);

            // xxx(okachaiev): not sure if this is an optimal approach
            ctx.pipeline().remove(PgwireStartupMessageDecoder.class);
            ctx.pipeline().addFirst(new PgwireMessageDecoder());
            ctx.writeAndFlush(new PgwireAuthenticationRequest());
        } else if (msg instanceof PgwireAuthenticationResponse) {
            // xxx(okachaiev): log
            System.out.println(msg);

            ctx.write(new PgwireAuthenticationOk());
            ctx.write(new PgwireParameterStatus("server_version", pgVersion));
            ctx.writeAndFlush(READY_FOR_QUERY_MESSAGE);
        } else if (msg instanceof PgwireQuery) {
            System.out.println("Executing query: " + msg);

            executeQuery(ctx, (PgwireQuery) msg);
        } else if (msg instanceof PgwireTerminate) {
            ctx.close();
        }
    }

    private void executeQuery(ChannelHandlerContext ctx, PgwireQuery sql) {
        try (final AvaticaPreparedStatement statement =
                (AvaticaPreparedStatement) this.conn.prepareStatement(sql.query)) {
            if (statement.getStatementType().equals(Meta.StatementType.SELECT)) {
                try (final ResultSet resultSet = statement.executeQuery()) {
                    sendQueryResult(ctx, resultSet);
                }
            } else {
                System.out.println("Abort execution: non SELECT statement");
                sendError(ctx, ERROR_CODE_INVALID_SQL_STATEMENT, ERROR_MESSAGE_NON_SELECT_STATEMENT);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // very generic "DATA EXCEPTION"
            // it would be increadibly hard to be precise with error codes
            // to make this happen we need to deep analysis over query/execution/parameters
            // and map all potential scenarios to a few dozens of error codes
            sendError(ctx, ERROR_CODE_DATA_EXCEPTION, e.getMessage());
        }
    }

    // send row descriptor
    // send row data
    // send command complete
    private void sendQueryResult(ChannelHandlerContext ctx, ResultSet resultSet) throws SQLException {
        final ResultSetMetaData metadata = resultSet.getMetaData();
        final int numColumns = metadata.getColumnCount();

        final List<PgwireField> fields = new ArrayList<>();
        for (int i=1; i <= numColumns; i++) {
            // xxx(okachaiev): in practice columns have different types
            fields.add(new PgwireVarcharField(metadata.getColumnName(i)));
        }
        ctx.write(new PgwireRowDescription(fields));

        while(resultSet.next()) {
            final Object[] row = new Object[numColumns];
            for (int i = 1; i <= numColumns; i++) {
                row[i-1] = resultSet.getObject(i);
            }
            ctx.write(new PgwireRowData(row));
        }

        ctx.write(SELECT_COMMAND_COMPLETE_MESSAGE);
        ctx.writeAndFlush(READY_FOR_QUERY_MESSAGE);
    }

    private void sendError(ChannelHandlerContext ctx, final String code, final String message) {
        ctx.write(new PgwireErrorMessage(PgwireErrorMessage.Severity.ERROR, code, message));
        // ready to serve again
        ctx.writeAndFlush(READY_FOR_QUERY_MESSAGE);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

}
