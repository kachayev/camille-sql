package pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class PgwireMessageDecoder extends LengthFieldBasedFrameDecoder {

    public PgwireMessageDecoder() {
        super(Integer.MAX_VALUE, 1, 4, -4, 0);
    }

    private String prepareQuery(ByteBuf queryContent) {
        byte[] bytes = new byte[queryContent.readableBytes()];
        queryContent.readBytes(bytes);
        String query = new String(bytes).strip();
        if (query.endsWith(";")) {
            return query.substring(0, query.lastIndexOf(";"));
        } else {
            return query;
        }
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        final byte typeCode = frame.readByte();
        final int msglen = frame.readInt();
        final ByteBuf content = frame.readBytes(msglen-4);
        switch(typeCode) {
            // xxx(okachaiev): there are quite a few more
            case 'p':
                return new PgwireAuthenticationResponse(content);
            case 'Q':
                return new PgwireQuery(prepareQuery(content));
            case 'X':
                return new PgwireTerminate();
        }
        return null;
    }
}
