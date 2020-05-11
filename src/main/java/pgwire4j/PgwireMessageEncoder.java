package pgwire4j;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

public class PgwireMessageEncoder extends MessageToMessageEncoder<PgwireServerMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, PgwireServerMessage msg, List<Object> out) throws Exception {
        out.add(msg.toByteBuf(ctx.alloc()));
    }

}
