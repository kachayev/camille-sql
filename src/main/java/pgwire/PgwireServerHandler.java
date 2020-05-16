package pgwire;

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
            ctx.writeAndFlush(new PgwireReadyForQuery());
        } else if (msg instanceof PgwireQuery) {
            // xxx(okachaiev): log
            System.out.println(msg);
            executeQuery(ctx, (PgwireQuery) msg);
        } else if (msg instanceof PgwireTerminate) {
            ctx.close();
        }
    }

    private void executeQuery(ChannelHandlerContext ctx, PgwireQuery sql) {
        try {
            try (
                final Statement statement = this.conn.createStatement();
                final ResultSet resultSet = statement.executeQuery(sql.query)
            ) {
                sendQueryResult(ctx, resultSet);
            }
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