package pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PgwireAuthenticationRequest extends PgwireServerMessage {
        @Override
        public ByteBuf toByteBuf(ByteBufAllocator allocator) {
            ByteBuf buf = allocator.buffer(9);
            buf.writeByte('R');
            buf.writeInt(8);
            buf.writeInt(3);
            return buf;
        }
    
}