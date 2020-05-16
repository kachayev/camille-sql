package pgwire;

import io.netty.buffer.ByteBuf;

public class PgwireQuery {

    public String query;

    public PgwireQuery(String query) {
        this.query = query;
    }

    public PgwireQuery(ByteBuf query) {
        byte[] bytes = new byte[query.readableBytes()];
        query.readBytes(bytes);
        this.query = new String(bytes);
    }

    public String toString() {
        return String.format("Query<sql='%s'>", query);
    }
}
