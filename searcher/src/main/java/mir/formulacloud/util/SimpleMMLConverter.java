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

    private static final Pattern STR_NODE_PATTERN = Pattern.compile(
            "([^\\s:,(]+)\\(|" +
            "([^\\s:,(]+):([^,]+|[:,)])|" +
            "(\\))"
    );

    private static final int NODE_START = 1;
    private static final int LEAF_TAG = 2;
    private static final int LEAF_CONTENT = 3;
    private static final int NODE_END = 4;

    public static String stringToMML(String str){
        Matcher matcher = STR_NODE_PATTERN.matcher(str);
        StringBuilder sb = new StringBuilder();

        stringToMML(sb, matcher);

        return sb.toString();
    }

    private static String stringToMMLNew(String str){
        LinkedList<String> tagsCache = new LinkedList<>();
//        Pattern tagPattern = Pattern.compile(",)")

        return "";
    }

    private static void stringToMML(StringBuilder sb, Matcher matcher){
        while(matcher.find()){
            if (matcher.group(NODE_END) != null) return;

            if (matcher.group(LEAF_TAG) != null){
                String content = matcher.group(LEAF_CONTENT);
                content = content.matches("ivt") ? ""+INVISIBLE_TIMES : content;
                content = content.matches("fap") ? ""+FUNCTION_APPLY : content;
                content = StringEscapeUtils.escapeXml11(content);

                openTag(sb, matcher.group(LEAF_TAG));
                sb.append(content);
                closeTag(sb, matcher.group(LEAF_TAG));
            } else if (matcher.group(NODE_START) != null){
                String tag = matcher.group(NODE_START);
                openTag(sb, tag);
                stringToMML(sb, matcher);
                closeTag(sb, tag);
            }
        }
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
        sb.append(stringToMML(mathElement.getExpression()));
        sb.append("\n  </math>\n");
        sb.append("</mws:expr>");
        return sb.toString();
    }

    public static String escapeDoubleQuotes(String str){
        return str.replaceAll("\"", "DOUBLE_QUOTE");
    }

//    public static String minimizeString(String str){
//        Matcher matcher = STR_NODE_PATTERN.matcher(str);
//        StringBuilder outSB = new StringBuilder();
//        while( matcher.find() ){
//            if (matcher.group(LEAF_TAG) != null){
//                String content = matcher.group(LEAF_CONTENT);
//                content = content.matches("ivt") ? ""+INVISIBLE_TIMES : content;
//                content = content.matches("fap") ? ""+FUNCTION_APPLY : content;
//                outSB.append(content);
//            }
//        }
//        return outSB.toString();
//    }

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

        String test = "mrow(mtext:(1.3)–(1.4),mo:a,msub(mi:g,mi:N))";
        System.out.println(test);
        System.out.println(stringToMML(test));

        String test2 = "mrow(mtext:(1.3), (1.4))";
        System.out.println();
        System.out.println(test2);
        System.out.println(stringToMML(test2));

//        Path in = Paths.get("/opt/zbmath/tfidf/");
//        Path out = Paths.get("/opt/zbmath/tfidf-data/");
//        TFIDFLoader.initTFIDFLoader(in);
//
//        ForkJoinPool writerPool = new ForkJoinPool();
//
//        int parall = 12;
//        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
//        for ( int i = 0; i < parall; i++ ){
//            Path outF = out.resolve((i+1)+".xml");
//            BasexDataWriter writer = new BasexDataWriter(outF, queue);
//            writerPool.submit(writer);
//        }
//
//        int[] counter = new int[]{0};
//        Stream<MathElement> elementStream = TFIDFLoader.getLoaderInstance().getMathElementStream();
//        elementStream.parallel()
//                .map( SimpleMMLConverter::toBaseXInfo )
//                .forEach( s -> {
//                    queue.add(s);
//                    counter[0]++;
//                    System.out.print("\r" + counter[0] + " [" + queue.size() + "]");
//                } );
//
//        LOG.info("Finished put all stuff into writing lists. Wait until all writers are done.");
//
//        for ( int i = 0; i < parall; i++ ){
//            queue.add("STOP");
//        }
//
//        try {
//            writerPool.shutdown();
//            writerPool.awaitTermination(12, TimeUnit.MINUTES);
//            LOG.info("Done!");
//        } catch (InterruptedException e) {
//            LOG.fatal("Interrupt writing process...", e);
//        }

//        String mml = Files.readAllLines(p).get(0);



//        System.out.println(mml.substring(1,50));
//        System.out.println(mml.length());
//        String min = minimizeString(mml);
//        System.out.println(min.length());
//        System.out.println(min);

    }

}
