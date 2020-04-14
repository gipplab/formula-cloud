package mir.formulacloud.beans;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author Andre Greiner-Petter
 */
public class Document {
    private static final Logger LOG = LogManager.getLogger(Document.class.getName());

    public static final String SPLITTER_DATA = " #-<>-# ";
    public static final String SPLITTER_ENTRY = System.lineSeparator();

    public static Pattern entryPattern = Pattern.compile(
            "^(.*)" + SPLITTER_DATA + "(.*)" + SPLITTER_DATA + "(.*)\\s*$"
    );

    public static final int IDX_EXPR    = 1;
    public static final int IDX_DEPTH   = 2;
    public static final int IDX_FREQ    = 3;


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

    /**
     * Expr -> Depth -> Freq
     * @param expression
     * @param depth
     * @param frequency
     */
    public void addFormula(String expression, Short depth, Short frequency) {
        this.expressions.addLast(cleanExpression(expression));
        this.depths.addLast(depth);
        this.termFrequencies.addLast(frequency);
    }

    public static String cleanExpression(String expression) {
        Pattern p = Pattern.compile(" [\\w:]+=\".*?\"|\\s*|\\n*");
        StringBuffer sb = new StringBuffer();
        Matcher m = p.matcher(expression);
        while ( m.find() ) {
            m.appendReplacement(sb, "");
        }
        m.appendTail(sb);
        return sb.toString();
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

    public String toSubexpressionsString() {
        StringBuffer sb = new StringBuffer();
        for ( int i = 0; i < expressions.size(); i++ ) {
            String expr = expressions.get(i);
            int tf = termFrequencies.get(i);
            for ( int j = 0; j < tf; j++ ) {
                sb.append(expr).append(System.lineSeparator());
            }
        }
        return sb.toString();
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

    public static Document parseDocument(Path p) {
        Document d = new Document();

        // properly release resource
        // see also: https://stackoverflow.com/questions/43067269/java-8-path-stream-and-filesystemexception-too-many-open-files
        try ( Stream<String> lines = Files.lines(p) ){
            lines.forEach( l -> {
                Matcher matcher = entryPattern.matcher(l);
                if ( matcher.matches() ){
                    d.addFormula(
                            matcher.group(IDX_EXPR),
                            Short.parseShort(matcher.group(IDX_DEPTH)),
                            Short.parseShort(matcher.group(IDX_FREQ))
                    );
                }
            });
        } catch ( IOException ioe ){
            LOG.error("Cannot read lines from file " + p.toString());
        }
        return d;
    }
}
