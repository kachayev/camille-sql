package pgwire4j;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PgwireRowData extends PgwireServerMessage {

    final Object[] fieldValues;

    public PgwireRowData(Object[] fieldValues) {
        this.fieldValues = fieldValues;
    }

    @Override
    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        int totalLength = 0;
        for (Object fieldValue: fieldValues) {
            // xxx(okachaiev): this is extremely inefficient :(
            totalLength += 4 + fieldValue.toString().length();
        }

        ByteBuf buf = allocator.buffer(7 + totalLength);
        buf.writeByte('D');
        buf.writeInt(6 + totalLength);
        buf.writeShort((short) fieldValues.length);
        for (Object fieldValue: fieldValues) {
            buf.writeInt(fieldValue.toString().length());
            buf.writeBytes(fieldValue.toString().getBytes());
        }
        return buf;
    }

}
