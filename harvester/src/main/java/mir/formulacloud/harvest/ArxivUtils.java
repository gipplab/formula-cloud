package mir.formulacloud.harvest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Andre Greiner-Petter
 */
public class ArxivUtils {

    private static final String OLD_ID_PATTERN = "[A-Za-z\\-]+/\\d+";
    private static final Pattern OLD_ID_PATTERN_DATASET = Pattern.compile("([A-Za-z\\-]+)(\\d+)");

    public static String convertFileNameToURLEntity(String fname){
        Matcher m = OLD_ID_PATTERN_DATASET.matcher(fname);
        if (m.matches()){ // old file name
            return m.group(1) + "/" + m.group(2);
        } else {
            return fname;
        }
    }

    public static String convertURLEntityToFileID(String urlEntity){
        if ( OLD_ID_PATTERN.matches(urlEntity) ){
            String[] parts = urlEntity.split("/");
            return parts[0] + parts[1];
        } else {
            return urlEntity;
        }
    }
}
