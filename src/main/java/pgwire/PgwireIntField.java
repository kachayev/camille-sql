package pgwire;

public class PgwireIntField extends PgwireField {
    public PgwireIntField (String fieldName) {
        super(fieldName, 23, (short) 4);
    }
}