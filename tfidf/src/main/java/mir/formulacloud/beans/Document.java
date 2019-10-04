package mir.formulacloud.beans;

import java.util.LinkedList;

/**
 * @author Andre Greiner-Petter
 */
public class Document {

    public static final String SPLITTER_DATA = " #-<>-# ";
    public static final String SPLITTER_ENTRY = " <-#-> ";

    private String DB;
    private String filename;

    private LinkedList<String> expressions;
    private LinkedList<Short> depths;
    private LinkedList<Short> termFrequencies;

    public Document () {}

    public Document ( String db, String filename ) {
        this.DB = db;
        this.filename = filename;

        expressions = new LinkedList<>();
        termFrequencies = new LinkedList<>();
    }

    public void addFormula(String expression, Short depth, Short frequency) {
        this.expressions.addLast(expression);
        this.depths.addFirst(depth);
        this.termFrequencies.addLast(frequency);
    }

    public boolean isNull(){
        return DB == null || filename == null;
    }

    public boolean isEmpty(){
        return expressions.isEmpty();
    }

    @Override
    public String toString() {
        StringBuffer out = new StringBuffer();
        for ( int i = 0; i < expressions.size(); i++ ){
            out.append(expressions.get(i));
            out.append(SPLITTER_DATA);
            out.append(depths.get(i));
            out.append(SPLITTER_DATA);
            out.append(termFrequencies.get(i));
            out.append(SPLITTER_ENTRY);
        }
        return out.toString();
    }


    public String getDB() {
        return DB;
    }

    public void setDB(String DB) {
        this.DB = DB;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public LinkedList<String> getExpressions() {
        return expressions;
    }

    public void setExpressions(LinkedList<String> expressions) {
        this.expressions = expressions;
    }

    public LinkedList<Short> getTermFrequencies() {
        return termFrequencies;
    }

    public void setTermFrequencies(LinkedList<Short> termFrequencies) {
        this.termFrequencies = termFrequencies;
    }
}
