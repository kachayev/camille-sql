package pgwire4j;

public class PgwireTextField extends PgwireField {

    public PgwireTextField(String fieldName) {
        super(fieldName, 32, (short) -1);
	}
}