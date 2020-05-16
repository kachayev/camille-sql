package pgwire;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class PgwireRowDescription extends PgwireServerMessage {

    public final List<PgwireField> fields;

    public PgwireRowDescription(List<PgwireField> fields) {
        this.fields = fields;
    }

    @Override
    public ByteBuf toByteBuf(ByteBufAllocator allocator) {
        int totalLength = 0;
        for (PgwireField field: fields) {
            totalLength += field.fieldName.length() + 19;
        }

        ByteBuf buf = allocator.buffer(totalLength + 7);
        buf.writeByte('T');
        buf.writeInt(6 + totalLength);
        buf.writeShort((short) fields.size());

        for (PgwireField field: fields) {
            buf.writeBytes(field.fieldName.getBytes());
            buf.writeZero(1);
            buf.writeInt(0);
            buf.writeShort(0);
            buf.writeInt(field.typeId);
            buf.writeShort((short) field.typeSize);
            buf.writeInt(-1);
            buf.writeShort(0);
        }

        return buf;
    }

}
