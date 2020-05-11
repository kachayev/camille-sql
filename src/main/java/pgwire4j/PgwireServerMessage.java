package pgwire4j;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public abstract class PgwireServerMessage {
    abstract public ByteBuf toByteBuf(ByteBufAllocator allocator);
}
