package pgwire4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import m2sql.MavenArtifactsDatabase;

public class PgwireServerHandler extends SimpleChannelInboundHandler<Object> {

    private final MavenArtifactsDatabase db;
    private Connection conn;

    public PgwireServerHandler(MavenArtifactsDatabase db) {
        super();
        this.db = db;
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
            System.out.println(msg);
            // xxx(okachaiev): not sure if this is optimal approach
            ctx.pipeline().remove(PgwireStartupMessageDecoder.class);
            ctx.pipeline().addFirst(new PgwireMessageDecoder());
            ctx.writeAndFlush(new PgwireAuthenticationRequest());
        } else if (msg instanceof PgwireAuthenticationResponse) {
            System.out.println(msg);
            ctx.write(new PgwireAuthenticationOk());
            ctx.writeAndFlush(new PgwireReadyForQuery());
        } else if (msg instanceof PgwireQuery) {
            System.out.println(msg);
            executeQuery(ctx, (PgwireQuery) msg);
        } else if (msg instanceof PgwireTerminate) {
            ctx.close();
        }
    }

    private void executeQuery(ChannelHandlerContext ctx, PgwireQuery sqlQuery) {
        try {
            final Statement statement = this.conn.createStatement();
            String sql;
            if (sqlQuery.query.endsWith(";")) {
                sql = sqlQuery.query.substring(0, sqlQuery.query.lastIndexOf(";"));
            } else {
                sql = sqlQuery.query;
            }
            final ResultSet resultSet = statement.executeQuery(sql);
            sendQueryResult(ctx, resultSet);
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            ctx.close();
        }
    }

    // send row descriptor
    // send row data
    // send command complete
    private void sendQueryResult(ChannelHandlerContext ctx, ResultSet resultSet) {
        try {
            final ResultSetMetaData metadata = resultSet.getMetaData();
            final int numColumns = metadata.getColumnCount();

            final List<PgwireField> fields = new ArrayList<>();
            for (int i=1; i <= numColumns; i++) {
                // xxx(okachaiev): columns of different type, com'n
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

            ctx.write(new PgwireCommandComplete());
            ctx.write(new PgwireReadyForQuery());
            ctx.flush();
        } catch (SQLException e) {
            ctx.fireExceptionCaught(e);
        }
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
