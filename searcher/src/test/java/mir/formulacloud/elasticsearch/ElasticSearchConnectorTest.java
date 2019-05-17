package mir.formulacloud.elasticsearch;

import mir.formulacloud.searcher.SearcherConfig;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * @author Andre Greiner-Petter
 */
public class ElasticSearchConnectorTest {

    private static ElasticSearchConnector es;

    @BeforeAll
    public static void init(){
        es = new ElasticSearchConnector(new SearcherConfig());
        es.start();
    }

    @AfterAll
    public static void finish() {
        es.stop();
    }

    @Test
    public void simpleQueryTest(){
        String testQuery = "cosine function";
        SearchHits shits = es.search(testQuery, "zbmath");
        SearchHit[] hits = shits.getHits();
        printHits(hits);
    }

    @Test
    public void statTest(){
        es.numberOfDocuments("arxiv-no-problem");
    }

    @Test
    public void advancedSearchTest(){
        String searchQuery = "power law distribution ecosystem";
        SearchHits searchHits = es.search(searchQuery, "arxiv-no-problem");
        SearchHit[] hitsArr = searchHits.getHits();
        printHits(hitsArr);
    }

    @Test
    public void getTest(){
        GetResponse res = es.getID("1046592");
        Map<String, Object> source = res.getSource();
        if (source != null){
            System.out.println(source.get("title"));
            System.out.println(source.get("database"));
        }

        System.out.println(res);
    }

    private void printHits(SearchHit... hits){
        for (SearchHit hit : hits) {
            System.out.println(hit);
            System.out.println(hit.getId());
            System.out.println(hit.getScore());
            Map<String, Object> source = hit.getSourceAsMap();
            System.out.println(source.get("database"));
            System.out.println(source.get("arxiv"));
            System.out.println(source.get("title"));
        }
    }
}
