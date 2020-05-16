package pgwire;

public class PgwireField {
    public final String fieldName;
    public final int typeId;
    public final short typeSize;

    public PgwireField(String fieldName, int typeId, short i) {
        this.fieldName = fieldName;
        this.typeId = typeId;
        this.typeSize = i;
    }
}