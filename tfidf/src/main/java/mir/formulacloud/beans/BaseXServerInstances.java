package mir.formulacloud.beans;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.basex.BaseXServer;

import java.io.IOException;

/**
 * @author Andre Greiner-Petter
 */
public class BaseXServerInstances {
    private static final Logger LOG = LogManager.getLogger(BaseXServerInstances.class.getName());

    private String database;
    private int port;
    private Thread thread;
    private Server serverRunnable;

    public BaseXServerInstances(String database, int port){
        this.database = database;
        this.port = port;
    }

    public void start() {
        if (thread != null) return;
        serverRunnable = new Server(port);
        thread = new Thread(serverRunnable);
        thread.start();
    }

    public void stop() {
        serverRunnable.stop();
    }

    public String getDatabase() {
        return database;
    }

    public int getPort() {
        return port;
    }

    private static class Server implements Runnable {
        private BaseXServer server;

        public Server(int port){
            try {
                server = new BaseXServer("-p" + port);
            } catch (IOException e) {
                LOG.error("Cannot instantiate BaseXServer.", e);
                System.exit(1);
            }
        }

        @Override
        public void run() {
            server.run();
        }

        public void stop() {
            server.stop();
        }
    }
}
