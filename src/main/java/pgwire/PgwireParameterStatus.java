package pgwire;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PgwireParameterStatus extends PgwireServerMessage {

    private final String parameterKey;
    private final String parameterValue;

    @Override
    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        final int dataSize = parameterKey.getBytes().length + parameterValue.getBytes().length + 2;
        final ByteBuf buf = allocator.buffer(dataSize+5);
        buf.writeByte('S');
        buf.writeInt(dataSize+4);
        buf.writeBytes(parameterKey.getBytes());
        buf.writeZero(1);
        buf.writeBytes(parameterValue.getBytes());
        buf.writeZero(1);
        return buf;
    }

}