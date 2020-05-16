package pgwire;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import m2sql.MavenArtifactsDatabase;

public class PgwireServer 
{
    final static int DEFAULT_PORT = 26727;

    final static String DEFAUL_FOLDER = System.getProperty("user.home") + "/.m2/repository/";

    public static void main(String[] args) throws InterruptedException
    {
        final MavenArtifactsDatabase db = new MavenArtifactsDatabase(DEFAUL_FOLDER);

        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new PgwireServerInitializer(db));
            b.bind(DEFAULT_PORT).sync().channel().closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }
}
