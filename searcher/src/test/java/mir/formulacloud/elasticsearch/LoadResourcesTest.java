package mir.formulacloud.elasticsearch;

import mir.formulacloud.util.XQueryLoader;
import org.junit.jupiter.api.Test;

/**
 * @author Andre Greiner-Petter
 */
public class LoadResourcesTest {
    @Test
    public void loadResources(){
        XQueryLoader.initMinTermFrequency(5);
    }
}
