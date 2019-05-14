package mir.formulacloud.searcher;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Andre Greiner-Petter
 */
public class SearcherConfig {
    @Parameter(names = {"-tfidf", "--frequencyTables"}, description = "Specify the location of the TF-IDF dataset", required = true)
    private String tfidfData = "";

    @Parameter(names = {"-db", "--databaseFolder"}, description = "Specify the location of parent folder of the BaseX databases.", required = true)
    private String databaseParentFolder = "";

    @Parameter(names = {"-index", "--elasticsearchIndex"}, description = "Use this value to direct all requests to a specific index in ES.")
    private String fixedIndex = "";

    @Parameter(names = {"-esHost", "--elasticsearchHost"}, description = "Set the host of the elasticsearch server (default is localhost).")
    private String elasticsearchHost = "localhost";

    @Parameter(names = {"-esPort", "--elasticsearchPort"}, description = "Set the port of the elasticsearch server (default is 9200).")
    private int elasticsearchPort = 9200;

    @Parameter(names = {"-esMaxHits", "--elasticsearchMaximumHits"}, description = "Set the maximum number of hits from elasticsearch (default is 10).")
    private int elasticsearchMaxHits = 10;

    @Parameter(names = {"-minTF", "--minTermFrequency"}, description = "Set the min term frequency (should be the same as used for creating the TF-IDF values / Default: 5).")
    private int minTermFrequency = 1;

    @Parameter(names = {"-minDF", "--minDocumentFrequency"}, description = "Set the min document frequency (default is 1).")
    private int minDocumentFrequency = 1;

    @Parameter(names = {"-maxDF", "--maxDocumentFrequency"}, description = "Set the max document frequency (default is 1).")
    private int maxDocumentFrequency = Integer.MAX_VALUE;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help = false;

    public SearcherConfig(){}

    public Path getTfidfData() {
        return Paths.get(tfidfData);
    }

    public String getDatabaseParentFolder() {
        return databaseParentFolder;
    }

    public String getElasticsearchHost() {
        return elasticsearchHost;
    }

    public int getElasticsearchPort() {
        return elasticsearchPort;
    }

    public int getElasticsearchMaxHits() {
        return elasticsearchMaxHits;
    }

    public int getMinTermFrequency() {
        return minTermFrequency;
    }

    public int getMinDocumentFrequency() {
        return minDocumentFrequency;
    }

    public String getFixedIndex() {
        return fixedIndex;
    }

    public boolean useFixedIndex(){
        return !fixedIndex.isEmpty();
    }

    protected void setTfidfData(String tfidfData) {
        this.tfidfData = tfidfData;
    }

    protected void setDatabaseParentFolder(String databaseParentFolder) {
        this.databaseParentFolder = databaseParentFolder;
    }

    protected void setFixedIndex(String fixedIndex) {
        this.fixedIndex = fixedIndex;
    }

    protected void setElasticsearchHost(String elasticsearchHost) {
        this.elasticsearchHost = elasticsearchHost;
    }

    protected void setElasticsearchPort(int elasticsearchPort) {
        this.elasticsearchPort = elasticsearchPort;
    }

    protected void setElasticsearchMaxHits(int elasticsearchMaxHits) {
        this.elasticsearchMaxHits = elasticsearchMaxHits;
    }

    protected void setMinTermFrequency(int minTermFrequency) {
        this.minTermFrequency = minTermFrequency;
    }

    protected void setMinDocumentFrequency(int minDocumentFrequency) {
        this.minDocumentFrequency = minDocumentFrequency;
    }

    public int getMaxDocumentFrequency() {
        return maxDocumentFrequency;
    }

    public void setMaxDocumentFrequency(int maxDocumentFrequency) {
        this.maxDocumentFrequency = maxDocumentFrequency;
    }

    public boolean isHelp() {
        return help;
    }

    public static SearcherConfig loadConfig(String[] args){
        // parse config
        SearcherConfig config = new SearcherConfig();

        JCommander jcommander = JCommander
                .newBuilder()
                .addObject(config)
                .build();

        if (args == null || args.length < 1){
            jcommander.usage();
            System.exit(0);
        }

        jcommander.parse(args);

        if (config.isHelp()){
            jcommander.usage();
            System.exit(0);
        }
        return config;
    }
}
