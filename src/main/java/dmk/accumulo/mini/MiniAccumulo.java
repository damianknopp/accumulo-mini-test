package dmk.accumulo.mini;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
//        String tmp = System.getProperty("java.io.tmpdir");
//        Path tmpPath = Paths.get(tmp, "mini-accumulo");
        Path tmpPath = Files.createTempDirectory("mini-accumulo");
        File tmpDir = tmpPath.toFile();
        tmpDir.deleteOnExit();
        MiniAccumuloConfig mac = new MiniAccumuloConfig(tmpDir, "secret");
        mac.setZooKeeperPort(2181);
//        Map<String, String> siteConfig = mac.getSiteConfig();
//        siteConfig.put(org.apache.accumulo.core.conf.Property.INSTANCE_ZK_HOST.getKey(),  "0.0.0.0:2181");
//        mac.setSiteConfig(siteConfig);
        accumulo = new MiniAccumuloCluster(mac);
        MiniAccumuloConfig config = accumulo.getConfig();
        // If Java 9+ then fix the classpath!
        setConfigClassPath(config);
        modifyZkConfg(tmpPath);
        logger.info("creating mini accumulo in " + tmpPath.toString());
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
               logger.info("stopping accumulo");
               accumulo.stop();
               logger.info("done");
               logger.info("purging timer tasks");
               stayAliveTimer.purge();
               logger.info("done");
               logger.info("canceling timer tasks");
               stayAliveTimer.cancel();
               logger.info("done");
           } catch (Exception e) {
               e.printStackTrace();
           }
        });
        Runtime.getRuntime().addShutdownHook(shutdown);

    }

    /**
     * all zk to bind to all interface to listen to just localhost
     * @param tmp
     * @throws IOException
     */
    public void modifyZkConfg(Path tmp) throws IOException {
        Path zkConf = tmp.resolve("conf/zoo.cfg");
        String original = "clientPortAddress=127.0.0.1";
        String replace = "clientPortAddress=0.0.0.0";
        try (Stream<String> lines = Files.lines(zkConf)) {
            List<String> replaced = lines
                    .map(line-> line.replaceAll(original, replace))
                    .collect(Collectors.toList());
            Files.write(zkConf, replaced);
        }
    }

    private static class KeepAliveTimerTask extends TimerTask {
        static Logger logger = LoggerFactory.getLogger(MiniAccumulo.class);
        public void run() {
            logger.debug("stay alive timer task");
        }
    }
}