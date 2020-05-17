package pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public abstract class PgwireServerMessage extends Object {
    abstract public ByteBuf toByteBuf(ByteBufAllocator allocator);
}
