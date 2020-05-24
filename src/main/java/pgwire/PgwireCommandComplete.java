package pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PgwireCommandComplete extends PgwireServerMessage {

    private final String command;

    private final int markedLength;

    public PgwireCommandComplete(final String command) {
        this.command = command;
        this.markedLength = 4 + 1 + command.length();
    }

    @Override
    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        final ByteBuf buf = allocator.buffer(markedLength + 1);
        buf.writeByte('C');
        buf.writeInt(markedLength);
        buf.writeBytes(command.getBytes());
        buf.writeZero(1);
        return buf;
    }

}