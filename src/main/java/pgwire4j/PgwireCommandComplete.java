package pgwire4j;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PgwireCommandComplete extends PgwireServerMessage {

    // xxx(okachaiev): that different for different completions
    static final String command = "SELECT";

    @Override
    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        ByteBuf buf = allocator.buffer(12);
        buf.writeByte('C');
        buf.writeInt(11);
        buf.writeBytes(command.getBytes());
        buf.writeZero(1);
        return buf;
    }

}