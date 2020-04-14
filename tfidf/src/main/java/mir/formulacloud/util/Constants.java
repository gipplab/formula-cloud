package mir.formulacloud.util;

import java.util.regex.Pattern;

/**
 * @author Andre Greiner-Petter
 */
public class Constants {
    private Constants(){}

    public static final int BX_IDX_EXPR  = 3;
    public static final int BX_IDX_DEPTH = 2;
    public static final int BX_IDX_FREQ  = 1;

    public static final Pattern BASEX_ELEMENT_PATTERN =
            Pattern.compile("<element.*freq=\"(\\d+)\" depth=\"(\\d+)\">(.*?)</element>");

    public static final Pattern BASEX_ELEMENT_XML_PATTERN = Pattern.compile(
            "<element.*?freq=\"(\\d+)\" depth=\"(\\d+)\">(.*?)</element>", Pattern.DOTALL
    );
}
