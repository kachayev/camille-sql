package pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

// xxx(okachaiev): fix length field decoder
// xxx(okachaiev): read params, not only the version
public class PgwireStartupMessageDecoder extends LengthFieldBasedFrameDecoder {
    public PgwireStartupMessageDecoder() {
        super(Integer.MAX_VALUE, 0, 4, 0, 0);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        final int msglen = in.readInt();
        final int version = in.readInt();
        in.readBytes(msglen-8);
        // ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        // if (frame == null) {
        //     return null;
        // }

        // final int v = frame.readInt();
        return new PgwireStartupMessage(version >> 16, version & 0xffff);
    }

}
