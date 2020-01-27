package mir.formulacloud.util;

import mir.formulacloud.beans.MathElement;
import mir.formulacloud.tfidf.Writer;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.basex.query.func.math.MathE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author Andre Greiner-Petter
 */
public class SimpleMMLConverter {
    private static final Logger LOG = LogManager.getLogger(SimpleMMLConverter.class.getName());

    private SimpleMMLConverter(){}

    public static final char FUNCTION_APPLY = '\u2061';
    public static final char INVISIBLE_TIMES = '\u2062';

    private static final Pattern ANY_PATTERN = Pattern.compile("([a-z]+)([(:])");

    /**
     * Converts the string formatted equation to MathML formatted equation.
     * @param str string formatted equation
     * @return MathML formatted equation
     */
    public static String stringToMML(String str){
        StringBuilder sb = new StringBuilder();
        stringToMML(sb, str);
        return sb.toString();
    }

    /**
     * Converts the string math equation to MathML and fills the given StringBuilder
     * with the MathML format. {@link #stringToMML(String)} might be more convenient.
     * @param sb StringBuilder will be filled with MathML formatted {@param str}
     * @param str the math equation in string format that will be formatted to MathML
     */
    public static void stringToMML(StringBuilder sb, String str) {
        LinkedList<String> stack = new LinkedList<>();

        if ( str.isEmpty() ) {
            return;
        }

        Matcher any = ANY_PATTERN.matcher(str);
        while ( any.find() ) {
            StringBuffer restBuffer = new StringBuffer();
            any.appendReplacement(restBuffer, "");
            String prev = restBuffer.toString();

            if ( !stack.isEmpty() && stack.getLast().equals(":mtext") ){
                prev = prev.substring(0, Math.max(0,prev.length()-1));

                if ( prev.contains("(") || prev.contains(")") ) {
                    String tmp = "";
                    LinkedList<String> open = new LinkedList<>();
                    for ( int i = 0; i < prev.length(); i++) {
                        if ( (prev.charAt(i)+"").equals("(") ) {
                            open.addLast("(");
                        }
                        else if ( (prev.charAt(i)+"").equals(")") ) {
                            if ( open.isEmpty() ) {
                                break;
                            }
                            open.removeLast();
//                            if ( open.isEmpty() ) {
//                                tmp += ")";
//                                break;
//                            }
                        }

                        tmp += prev.charAt(i);
                    }

                    prev = prev.substring(tmp.length());
                    sb.append(cleanContent(tmp));
                    closeTag(sb, "mtext");
                    stack.removeLast();
                } else {
                    sb.append(cleanContent(prev));
                    closeTag(sb, "mtext");
                    stack.removeLast();
                    prev = "";
                }
            }

            if ( prev.length()>0 ) {
                int startIndex = 0;
                String buffer = "";

                if ( !stack.isEmpty() && stack.getLast().startsWith(":") ){
                    if ( stack.getLast().startsWith(":") && prev.matches("^,(?:[^,)].*|$)") ) {
                        // empty mo... very special case
                        String tag = stack.removeLast();
                        closeTag(sb, tag.substring(1));
                        prev = "";
                    } else {
                        startIndex++;
                        buffer += prev.charAt(0);
                    }
                }

                for ( int i = startIndex; i < prev.length(); i++) {
                    String e = ""+prev.charAt(i);
                    if ( e.equals(")") ) {
                        if ( !stack.isEmpty() ){
                            String lastTag = stack.removeLast();
                            if ( lastTag.startsWith(":") ) {
                                sb.append(cleanContent(buffer));
                                closeTag(sb, lastTag.substring(1));
                                lastTag = stack.removeLast();
                            }
                            closeTag(sb, lastTag);
                        } else {
                            LOG.error("Hows that possible??");
                        }
                    } else if ( e.equals(",") ) {
                        String lastTag = stack.removeLast();
                        if ( lastTag.startsWith(":") ) {
                            sb.append(cleanContent(buffer));
                            closeTag(sb, lastTag.substring(1));
                        } else {
                            stack.addLast(lastTag);
                        }
                    } else {
                        buffer += e;
                    }
                }
            }

            String tmp = any.group(2);
            String tag = any.group(1);

            if ( tmp.equals(":") ) {
                // is an element
                stack.addLast(":"+tag);
                openTag(sb, tag);
            } else {
                // is a parent node
                stack.addLast(tag);
                openTag(sb, tag);
            }
        }

        StringBuffer restBuffer = new StringBuffer();
        any.appendTail(restBuffer);
        String rest = restBuffer.toString();

        String lastElement = stack.removeLast();
        if ( lastElement.startsWith(":") ) {
            rest = rest.substring(0, Math.max(0,rest.length()-stack.size()));
            sb.append(cleanContent(rest));
            closeTag(sb, lastElement.substring(1));
        } else {
            rest = rest.substring(0, Math.max(0,rest.length()-(stack.size()+1)));
            sb.append(cleanContent(rest));
            closeTag(sb, lastElement);
        }

        while ( !stack.isEmpty() ) {
            String tag = stack.removeLast();
            if ( tag.startsWith(":") ) tag = tag.substring(1);
            closeTag(sb, tag);
        }
    }

    private static String cleanContent( String content ) {
        content = content.matches("ivt") ? "" + INVISIBLE_TIMES : content;
        content = content.matches("fap") ? "" + FUNCTION_APPLY : content;
        content = StringEscapeUtils.escapeXml11(content);
        return content;
    }

    private static void openTag(StringBuilder sb, String tag){
        sb.append("<");
        sb.append(tag);
        sb.append(">");
    }

    private static void closeTag(StringBuilder sb, String tag){
        sb.append("</");
        sb.append(tag);
        sb.append(">");
    }

    public static String[] outerTags() {
        return new String[]{
                "<mws:harvest xmlns:mws=\"http://search.mathweb.org/ns\">\n",
                "\n</mws:harvest>"
        };
    }

    public static String toBaseXInfo(MathElement mathElement){
        StringBuilder sb = new StringBuilder();
        sb.append("<mws:expr ");
        sb.append("complexity=\"").append(mathElement.getDepth()).append("\" ");
        sb.append("term-frequency=\"").append(mathElement.getTotalFrequency()).append("\" ");
        sb.append("document-frequency=\"").append(mathElement.getDocFrequency()).append("\" ");
        String str = escapeDoubleQuotes(mathElement.getExpression());
        sb.append("string=\"").append(str).append("\">\n");
        sb.append("  <math xmlns=\"http://www.w3.org/1998/Math/MathML\">\n    ");
//        sb.append(stringToMML(mathElement.getExpression()));
        try {
            String res = stringToMML(mathElement.getExpression());
            if ( !valid(res) ) {
                LOG.warn("Invalid MathML generated: " + mathElement.getExpression() + "\n" + res);
                return null;
            } else {
                sb.append(res);
            }
        } catch ( Exception e ) {
            LOG.warn("Error for " + mathElement.getExpression());
            return null;
        }
        sb.append("\n  </math>\n");
        sb.append("</mws:expr>");
        return sb.toString();
    }

    private static boolean valid(String in) {
        LinkedList<String> stack = new LinkedList<>();
        Matcher m = Pattern.compile("<(/?[a-z]+?)>").matcher(in);
        while ( m.find() ) {
            String el = m.group(1);
            if ( el.startsWith("/") ) {
                el = el.substring(1);
                if ( !stack.isEmpty() && stack.getLast().equals(el) ) {
                    stack.removeLast();
                } else {
                    return false;
                }
            } else {
                stack.addLast(el);
            }
        }
        return stack.isEmpty();
    }

    public static String escapeDoubleQuotes(String str){
        return str.replaceAll("\"", "DOUBLE_QUOTE");
    }

    public static void transformGroupOfFiles( String lineRegex, Path in, Path out ) throws IOException {
        Pattern linePattern = Pattern.compile(lineRegex);

        Files.walk(in)
                .filter(Files::isRegularFile)
                .forEach( p -> {
                    try (Stream<String> lines = Files.lines(p)) {
                        StringBuilder sb = new StringBuilder();
                        lines.forEach( l -> {
                            System.out.println(l);
                            Matcher m = linePattern.matcher(l);
                            if ( m.matches() ){
                                String mml = stringToMML(m.group(2));
                                sb.append(m.group(1) + "," + mml).append(System.lineSeparator());
                            } else {
                                System.out.println("WTF NO MATCH:");
                                System.out.println(l);
                            }
                        });

                        Path outF = out.resolve(p.getFileName());
                        Files.write(outF, sb.toString().getBytes());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    public static void main(String[] args) throws IOException {
//        Path in = Paths.get( "/home/andreg-p/Projects/19CikmFormulaCloud/stats/searcher-bm25/arxiv-tmp");
//        Path out = Paths.get("/home/andreg-p/Projects/19CikmFormulaCloud/stats/searcher-bm25/arxiv");
//        String linePattern = "(\\d+\\.\\d+),(.*)";
//        transformGroupOfFiles(linePattern, in, out);

//        String[] t = "mn:2)))".split("\\)");
//        String test0 = "mrow(msub(mi:E,mn:0),mo:=,mrow(mi:m,mo:ivt,msup(mi:c,mn:2)))";
//        System.out.println(test0);
//        System.out.println(stringToMML(test0));
//
//        String test = "mrow(mrow(mtext:(a)),mi:b)";
//        System.out.println(test);
//        System.out.println(stringToMML(test));
//
//        String test2 = "mrow(mtable(mtr(mtd(mrow(mtext:(A),mo:,mrow(mi:u,mo:ivt,mrow(mo:(,mi:x,mo:))))),mtd(mrow(mrow(mi:,mo:≤,mrow(mrow(mi:C,mo:ivt,mrow(mo:(,mi:x,mo:))),mo:+,mrow(munderover(mo:∑,mrow(mi:i,mo:=,mn:1),mi:n),mrow(msubsup(mo:∫,mn:0,msub(mi:x,mi:i)),mrow(msub(mi:a,mi:i),mo:ivt,mrow(mo:(,msub(mi:x,mi:i),mo:,,msub(mi:s,mi:i),mo:)),mo:ivt,msup(mi:u,mi:α),mo:ivt,mrow(mo:(,msub(mi:x,mi:i),mo:,,msub(mi:s,mi:i),mo:)),mo:ivt,mrow(mo:\uD835\uDC51,msub(mi:s,mi:i))))))),mo:,))),mtr(mtd(mrow(mtext:(B),mo:,mrow(mi:u,mo:ivt,mrow(mo:(,mi:x,mo:))))),mtd(mrow(mrow(mi:,mo:≤,mrow(mrow(mi:C,mo:ivt,mrow(mo:(,mi:x,mo:))),mo:+,mrow(munderover(mo:∑,mrow(mi:i,mo:=,mn:1),mi:n),mrow(msubsup(mo:∫,mn:0,msub(mi:x,mi:i)),mrow(msub(mi:a,mi:i),mo:ivt,mrow(mo:(,msub(mi:x,mi:i),mo:,,msub(mi:s,mi:i),mo:)),mo:ivt,msub(mi:g,mi:i),mo:ivt,mrow(mo:(,mrow(mi:u,mo:ivt,mrow(mo:(,msub(mi:x,mi:i),mo:,,msub(mi:s,mi:i),mo:))),mo:)),mo:ivt,mrow(mo:\uD835\uDC51,msub(mi:s,mi:i))))))),mo:,)))),mo:,mi:x)";
//        System.out.println();
//        System.out.println(test2);
//        System.out.println(stringToMML(test2));
//
//        String test3 = "mrow(mrow(mtext:(1.3), (1.4)),mi:3)";
//        System.out.println();
//        System.out.println(test3);
//        System.out.println(stringToMML(test3));

//        Path in = Paths.get("/opt/zbmath/tfidf/");
//        Path out = Paths.get("/opt/zbmath/tfidf-data/");

        Path in = Paths.get(args[0]);
        Path out = Paths.get(args[1]);

        TFIDFLoader.initTFIDFLoader(in);

        ForkJoinPool writerPool = new ForkJoinPool();

        int parall = 12;
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        for ( int i = 0; i < parall; i++ ){
            Path outF = out.resolve((i+1)+".xml");
            BasexDataWriter writer = new BasexDataWriter(outF, queue);
            writerPool.submit(writer);
        }

        int[] counter = new int[]{0};
        Stream<MathElement> elementStream = TFIDFLoader.getLoaderInstance().getMathElementStream();
        elementStream.parallel()
                .map( SimpleMMLConverter::toBaseXInfo )
                .filter( Objects::nonNull )
                .forEach( s -> {
                    queue.add(s);
                    counter[0]++;
                    System.out.print("\r" + counter[0] + " [" + queue.size() + "]");
                } );

        LOG.info("Finished put all stuff into writing lists. Wait until all writers are done.");

        for ( int i = 0; i < parall; i++ ){
            queue.add("STOP");
        }

        try {
            writerPool.shutdown();
            writerPool.awaitTermination(12, TimeUnit.MINUTES);
            LOG.info("Done!");
        } catch (InterruptedException e) {
            LOG.fatal("Interrupt writing process...", e);
        }

//        String mml = Files.readAllLines(p).get(0);
//
//        System.out.println(mml.substring(1,50));
//        System.out.println(mml.length());
//        String min = minimizeString(mml);
//        System.out.println(min.length());
//        System.out.println(min);

    }

}
