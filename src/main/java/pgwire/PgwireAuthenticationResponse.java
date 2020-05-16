package pgwire;

import io.netty.buffer.ByteBuf;

public class PgwireAuthenticationResponse {

    public final String password;

    public PgwireAuthenticationResponse(String password) {
        this.password = password;
    }

    public PgwireAuthenticationResponse(ByteBuf password) {
        byte[] bytes = new byte[password.readableBytes()];
        password.readBytes(bytes);
        this.password = new String(bytes);
    }

    public String toString() {
        return String.format("Authentication<password=%s>", password);
    }

}
