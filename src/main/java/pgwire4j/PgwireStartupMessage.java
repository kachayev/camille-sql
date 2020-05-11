package pgwire4j;

public class PgwireStartupMessage extends PgwireClientMessage {
    public int majorVersion;
    public int minorVersion;

    public PgwireStartupMessage(int major, int minor) {
        majorVersion = major;
        minorVersion = minor;
    }

    public String toString() {
        return String.format("Version<major=%s, minor=%s>", majorVersion, minorVersion);
    }
}
