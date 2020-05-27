package pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PgwireErrorMessage extends PgwireServerMessage {

    public enum Severity {
        PANIC, FATAL, LOG, ERROR, WARNING, NOTICE, INFO, DEBUG1
    }

    private final String severity;
    private final String code;
    private final String message;

    private final int totalLength;

    public PgwireErrorMessage(final Severity severity, final String code, final String message) {
        this.severity = severity.toString();
        this.code = code;
        this.message = message;
        this.totalLength = this.severity.length() + code.length() + message.length();
    }

    @Override
    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        final ByteBuf buf = allocator.buffer(12 + this.totalLength);
        buf.writeByte('E');
        buf.writeInt(11 + this.totalLength);
        buf.writeByte('S');
        buf.writeBytes(this.severity.getBytes());
        buf.writeZero(1);
        buf.writeByte('C');
        buf.writeBytes(this.code.getBytes());
        buf.writeZero(1);
        buf.writeByte('M');
        buf.writeBytes(this.message.getBytes());
        buf.writeZero(2);
        return buf;
    }
    
}