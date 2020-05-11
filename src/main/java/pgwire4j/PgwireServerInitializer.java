package pgwire4j;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import m2sql.MavenArtifactsDatabase;

public class PgwireServerInitializer extends ChannelInitializer<SocketChannel> {

    private final MavenArtifactsDatabase db;

    public PgwireServerInitializer(MavenArtifactsDatabase db) {
        super();
        this.db = db;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(new PgwireStartupMessageDecoder());
        pipeline.addLast(new PgwireMessageEncoder());
        pipeline.addLast(new PgwireServerHandler(this.db));
    }

}
