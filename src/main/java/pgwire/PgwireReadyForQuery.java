package pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PgwireReadyForQuery extends PgwireServerMessage {

    @Override
    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        ByteBuf buf = allocator.buffer(6);
        buf.writeByte('Z');
        buf.writeInt(5);
        buf.writeByte('I');
        return buf;
    }

}
