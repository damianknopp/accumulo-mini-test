package dmk.accumulo.mini;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.facebook.presto.accumulo.MiniAccumuloConfigUtil.setConfigClassPath;

public class MiniAccumulo {
    Logger logger = LoggerFactory.getLogger(MiniAccumulo.class);

    MiniAccumuloCluster accumulo;

    public static void main(String[] args) throws IOException, InterruptedException {
        MiniAccumulo miniAccumulo = new MiniAccumulo();
        miniAccumulo.spinupAndWait();
    }

    protected MiniAccumulo() {
        super();
    }

    protected void spinupAndWait() throws IOException, InterruptedException {
        spinup();
        waitForShutdownRequest();
    }

    protected void spinup() throws IOException, InterruptedException {
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        MiniAccumuloConfig mac = new MiniAccumuloConfig(tempDir, "secret");
        mac.setZooKeeperPort(2181);
        accumulo = new MiniAccumuloCluster(mac);
        MiniAccumuloConfig config = accumulo.getConfig();
        // If Java 9+ then fix the classpath!
        setConfigClassPath(config);
        accumulo.start();
        logger.info("mini accumulo started");
    }

    protected void waitForShutdownRequest() throws IOException, InterruptedException {
        logger.info("cluster running with instance name " + accumulo.getInstanceName()
                + " and zookeepers " + accumulo.getZooKeepers());
        logger.info("hit Ctl-C to shutdown ...");
        Timer stayAliveTimer = new Timer();
        stayAliveTimer.schedule(new KeepAliveTimerTask(), 5 * 1000, 60 * 1000);
        Thread shutdown = new Thread(() -> {
           try {
               logger.info("interrupt received, killing server...");
               accumulo.stop();
               stayAliveTimer.purge();
               stayAliveTimer.cancel();
               System.exit(1);
           } catch (Exception e) {
               e.printStackTrace();
           }
        });
        Runtime.getRuntime().addShutdownHook(shutdown);

    }

    private static class KeepAliveTimerTask extends TimerTask {
        static Logger logger = LoggerFactory.getLogger(MiniAccumulo.class);
        public void run() {
            logger.debug("stay alive timer task");
        }
    }
}