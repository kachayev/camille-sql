package pgwire4j;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PgwireAuthenticationOk extends PgwireServerMessage {

    @Override
    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        ByteBuf buf = allocator.buffer(9);
        buf.writeByte('R');
        buf.writeInt(8);
        buf.writeInt(0);
        return buf;
    }

}
