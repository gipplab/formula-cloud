package mir.formulacloud.beans;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Andre Greiner-Petter
 */
public class Document {
    public static final String SPLITTER_DATA = " #-<>-# ";
    public static final String SPLITTER_ENTRY = System.lineSeparator();

    public static Pattern entryPattern = Pattern.compile(
            "^(.*)" + SPLITTER_DATA + "(.*)" + SPLITTER_DATA + "(.*)\\s*$"
    );


    private String DB;
    private String filename;

    private LinkedList<String> expressions;
    private LinkedList<Short> depths;
    private LinkedList<Short> termFrequencies;

    public Document () {
        init();
    }

    public Document ( String db, String filename ) {
        this.DB = db;
        this.filename = filename;
        init();
    }

    private void init(){
        expressions = new LinkedList<>();
        termFrequencies = new LinkedList<>();
        depths = new LinkedList<>();
    }

    public void addFormula(String expression, Short frequency, Short depth) {
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

    public boolean isNotEmpty() {
        return !(isNull() || isEmpty());
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

    public String getFilename() {
        return filename;
    }

    public LinkedList<String> getExpressions() {
        return expressions;
    }

    public LinkedList<Short> getTermFrequencies() {
        return termFrequencies;
    }

    public LinkedList<Short> getDepths() {
        return depths;
    }

    public static Document parseDocument(Path p) throws IOException {
        Document d = new Document();
        Files.lines(p)
                .forEach( l -> {
                    Matcher matcher = entryPattern.matcher(l);
                    if ( matcher.matches() ){
                        d.addFormula(
                                matcher.group(1),
                                Short.parseShort(matcher.group(2)),
                                Short.parseShort(matcher.group(3))
                        );
                    }
                } );
        return d;
    }
}
