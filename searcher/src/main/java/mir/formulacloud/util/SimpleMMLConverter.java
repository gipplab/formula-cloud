package mir.formulacloud.util;

import mir.formulacloud.beans.MathElement;
import org.apache.commons.text.StringEscapeUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Andre Greiner-Petter
 */
public class SimpleMMLConverter {
    private SimpleMMLConverter(){}

    public static final char FUNCTION_APPLY = '\u2061';
    public static final char INVISIBLE_TIMES = '\u2062';

    private static final Pattern STR_NODE_PATTERN = Pattern.compile(
            "([^\\s:,(]+)\\(|" +
            "([^\\s:,(]+):([^:,)]+|[:,)])|" +
            "(\\))"
    );

    private static final int NODE_START = 1;
    private static final int NODE_END = 4;
    private static final int LEAF_TAG = 2;
    private static final int LEAF_CONTENT = 3;

    public static String stringToMML(String str){
        Matcher matcher = STR_NODE_PATTERN.matcher(str);
        StringBuilder sb = new StringBuilder();

        stringToMML(sb, matcher);

        return sb.toString();
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

    public static String toBaseXInfo(MathElement mathElement){
        StringBuilder sb = new StringBuilder();
        sb.append("<expr complexity=\"").append(mathElement.getDepth()).append("\" ");
        sb.append("term-frequency=\"").append(mathElement.getTotalFrequency()).append("\" ");
        sb.append("document-frequency=\"").append(mathElement.getDocFrequency()).append("\"");
        sb.append(">");
        sb.append(stringToMML(mathElement.getExpression()));
        sb.append("</expr>");
        return sb.toString();
    }

    public static String minimizeString(String str){
        Matcher matcher = STR_NODE_PATTERN.matcher(str);
        StringBuilder outSB = new StringBuilder();
        while( matcher.find() ){
            if (matcher.group(LEAF_TAG) != null){
                String content = matcher.group(LEAF_CONTENT);
                content = content.matches("ivt") ? ""+INVISIBLE_TIMES : content;
                content = content.matches("fap") ? ""+FUNCTION_APPLY : content;
                outSB.append(content);
            }
        }
        return outSB.toString();

//        Pattern p = Pattern.compile(",\\w+(:|\\()");
//        return str.replaceAll(",?\\w+([:(])|\\)+,", "");
    }

    public static void main(String[] args) throws IOException {
        Path p = Paths.get("./data/huge.txt");
        String mml = Files.readAllLines(p).get(0);

//        System.out.println(stringToMML(mml));
        System.out.println(mml.substring(1,50));
        System.out.println(mml.length());
        String min = minimizeString(mml);
        System.out.println(min.length());
        System.out.println(min);

    }

//    public static String mmlToString(String mml){
//        return "";
//    }

}
