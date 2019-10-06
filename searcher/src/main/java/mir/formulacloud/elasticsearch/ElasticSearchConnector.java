package mir.formulacloud.elasticsearch;

import mir.formulacloud.beans.MathDocument;
import mir.formulacloud.searcher.SearcherConfig;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

/**
 * @author Andre Greiner-Petter
 */
public class ElasticSearchConnector {
    private static final Logger LOG = LogManager.getLogger(ElasticSearchConnector.class.getName());

    private static final String[] INCLUDE_FIELDS = new String[] {"title", "database", "arxiv"};
    private static final String[] EXCLUDE_FIELDS = new String[] {"content"};

    private static final String SUGGESTION_ID = "suggest_similar_content";

    private RestHighLevelClient client;
    private SearcherConfig config;

    public ElasticSearchConnector(SearcherConfig config){
        this.config = config;
    }

    public void start(){
        if (client != null){
            LOG.warn("Restart elasticsearch connection.");
            stop();
        }

        LOG.info("Setup elasticsearch connection.");
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(
                                config.getElasticsearchHost(),
                                config.getElasticsearchPort(),
                                "http"
                        )
                )
        );
    }

    public void stop(){
        try {
            client.close();
        } catch (IOException e) {
            LOG.error("Cannot close elasticsearch connection.", e);
        }
    }

    public SearchRequest createEnhancedSearchRequest(String searchQuery, @NotNull String... indices){
        // must match the given searchQuery at least 50% (the half of words must match)
        MatchQueryBuilder matchQB = QueryBuilders.matchQuery("content", searchQuery);
        matchQB.minimumShouldMatch("50%");

        // the database must exist in the result list
        ExistsQueryBuilder existQB = QueryBuilders.existsQuery("database");
        ExistsQueryBuilder existIsEmptyQB = QueryBuilders.existsQuery("isempty");

        // it should match the searchQuery with a given sloppiness
        MatchPhraseQueryBuilder matchPhraseQB = QueryBuilders.matchPhraseQuery("content", searchQuery);
        matchPhraseQB.slop(10);

        // connect it
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        bqb.must(matchQB);
        bqb.must(existQB);           // only if it has a database attached
        bqb.mustNot(existIsEmptyQB); // only if it is not empty (contains an identifier)
        bqb.should(matchPhraseQB);

        SearchSourceBuilder sb = new SearchSourceBuilder();
        sb.query(bqb);
        sb.size(config.getElasticsearchMaxHits());
        sb.fetchSource(INCLUDE_FIELDS, EXCLUDE_FIELDS);

        // optional
//        addSuggestionService(sb, searchQuery);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indices);
        searchRequest.source(sb);

        return searchRequest;
    }

    public SearchHits search(String searchQuery, @NotNull String... indices){
        SearchRequest request = createEnhancedSearchRequest(searchQuery, indices);
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            return response.getHits();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        }
    }

    public GetResponse getID(String id){
        GetRequest gr = new GetRequest("zbmath", "_doc", id);
        try {
            GetResponse res = client.get(gr, RequestOptions.DEFAULT);
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public long numberOfDocuments(String index){
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());

            CountRequest countRequest = new CountRequest();
            countRequest.source(searchSourceBuilder);
            countRequest.indices(index);

            CountResponse r = client.count(countRequest, RequestOptions.DEFAULT);
            return r.getCount();
        } catch (IOException e) {
            LOG.fatal("Cannot request number of documents from elasticsearch.");
            System.exit(1);
            return 0; // funny... lets make intellij happy here :D
        }
    }

    private void addSuggestionService(SearchSourceBuilder sourceBuilder, String searchQuery){
        LOG.debug("Add a suggestion function.");
        SuggestionBuilder suggestBuilder = SuggestBuilders.termSuggestion("content").text(searchQuery);
        SuggestBuilder suggest = new SuggestBuilder();
        suggest.addSuggestion(SUGGESTION_ID, suggestBuilder);
        sourceBuilder.suggest(suggest);
    }

    private void logSuggestions(Suggest suggest){
        Object o = suggest.getSuggestion(SUGGESTION_ID);
        TermSuggestion ts = (TermSuggestion)o;
        List<TermSuggestion.Entry> entries = ts.getEntries();
        for ( TermSuggestion.Entry e : entries ){
            List<TermSuggestion.Entry.Option> options = e.getOptions();
            for ( TermSuggestion.Entry.Option opt : options ){
                LOG.info("Suggestion received: " + opt.toString());
            }
        }
    }
}
