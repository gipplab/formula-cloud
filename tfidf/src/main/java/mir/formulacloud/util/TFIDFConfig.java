package mir.formulacloud.util;

import com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFConfig implements Serializable {
    @Parameter(names = {"-in", "--input"}, description = "Specify the location of the harvest dataset", required = true)
    private String dataset = "";

    @Parameter(names = {"-out", "--output"}, description = "Specify the location for the output file. If none is set, the output goes to the console.")
    private String outputF = "";

    @Parameter(names = {"-t", "--threads"}, description = "How many parallel threads should be used?")
    private int parallelism = 1;

    @Parameter(names = {"-host", "--basexhost"}, description = "Set the host of the basexserver (default is localhost)")
    private String basexHost = "localhost";

    @Parameter(names = {"-defCli", "--defaultClients"}, description = "Set the the number of clients that should be started by default (default is 1)")
    private int defaultClients = 1;

    @Parameter(names = {"-minTF", "--minTermFrequency"}, description = "Set the minimum term frequency per document (default is 1)")
    private int minTermFrequency = 1;

    @Parameter(names = {"-numOutF", "--numberOutputFiles"}, description = "Set the number of output files (default is 8) only effective if the output is specified via -out.")
    private int numOfOutputFiles = 8;

    @Parameter(names = {"-h", "--help"}, help = true)
    private boolean help = false;

    public TFIDFConfig(){}

    public int getParallelism() {
        if (parallelism < 1){
            System.out.println("Negative or 0 parallelism is not allowed. Start in a single thread instead.");
            return 1;
        }
        return parallelism;
    }

    public int getMinTermFrequency() {
        if (minTermFrequency < 1){
            System.out.println("Negative or 0 term frequency per document is not allowed. Set the value back to 1.");
            return 1;
        }
        return minTermFrequency;
    }

    public String getBasexHost() {
        return basexHost;
    }

    public String getDataset() {
        return dataset;
    }

    public String getOutputF() {
        return outputF;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public void setOutputF(String outputF) {
        this.outputF = outputF;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public void setBasexHost(String basexHost) {
        this.basexHost = basexHost;
    }

    public void setMinTermFrequency(int minTermFrequency) {
        this.minTermFrequency = minTermFrequency;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public int getDefaultClients() {
        return defaultClients;
    }

    public void setDefaultClients(int defaultClients) {
        this.defaultClients = defaultClients;
    }

    public int getNumOfOutputFiles() {
        return numOfOutputFiles;
    }

    public void setNumOfOutputFiles(int numOfOutputFiles) {
        this.numOfOutputFiles = numOfOutputFiles;
    }
}
