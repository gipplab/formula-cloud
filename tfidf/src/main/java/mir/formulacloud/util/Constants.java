package mir.formulacloud.util;

import java.util.regex.Pattern;

/**
 * @author Andre Greiner-Petter
 */
public class Constants {
    private Constants(){}

    public static final Pattern BASEX_ELEMENT_PATTERN =
            Pattern.compile("<element.*freq=\"(\\d+)\" depth=\"(\\d+)\">(.*?)</element>");

}
