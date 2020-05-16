package pgwire;

public class PgwireVarcharField extends PgwireField {
    public PgwireVarcharField(String fieldName) {
        super(fieldName, 32, (short) -1);
	}
}