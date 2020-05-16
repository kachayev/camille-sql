package camille.sql;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import m2sql.MavenArtifactsDatabase;
import pgwire.PgwireServerInitializer;

public class CamilleSqlServer {
    final static int DEFAULT_PORT = 26727;

    final static String DEFAUL_FOLDER = System.getProperty("user.home") + "/.m2/repository/";

    final static CommandLineParser parser = new DefaultParser();

    final static Options parserOptions = new Options();

    static {
        parserOptions.addOption("p", "port", true, "port");
        parserOptions.addOption("r", "repository", true,
            "path to maven artifacts repository (defaults to ~/.m2/repository)");
    }

    public static void main(String[] args) throws InterruptedException {
        CommandLine cmd = null;
        try {
            cmd = parser.parse(parserOptions, args);
        } catch (ParseException e) {
            System.out.print("Arguments error: ");
            System.out.println(e.getMessage());
            System.exit(1);
        }

        int port = DEFAULT_PORT;
        if (cmd.hasOption("p")) {
            port = Integer.valueOf(cmd.getOptionValue("p"));
        }

        String folder = DEFAUL_FOLDER;
        if (cmd.hasOption("r")) {
            folder = cmd.getOptionValue("r").strip();
            final File f = new File(folder);
            if (!f.exists() || !f.isDirectory()) {
                System.out.println(String.format("Invalid reposiroty directory: %s", folder));
                System.exit(1);
            }
            folder = f.getAbsolutePath();
        }

        final MavenArtifactsDatabase db = new MavenArtifactsDatabase(folder);

        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new PgwireServerInitializer(db));

            System.out.println(String.format("Artifacts repository path: %s", folder));
            System.out.println(String.format("Running server on localhost:%d", port));

            b.bind(port).sync().channel().closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }
}
