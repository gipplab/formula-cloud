package mir.formulacloud.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Andre Greiner-Petter
 */
public class Helper {
    private Helper(){}

    public static <T> void collect(Map<String, T> ref, Map<String, List<T>> collection) {
        for (String key : ref.keySet()){
            if (!collection.containsKey(key)){
                collection.put(key, new LinkedList<>());
            }
            List<T> referenceList = collection.get(key);
            referenceList.add(ref.get(key));
        }
    }
}
