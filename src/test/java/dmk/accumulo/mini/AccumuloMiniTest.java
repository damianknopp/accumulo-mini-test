package dmk.accumulo.mini;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static com.facebook.presto.accumulo.MiniAccumuloConfigUtil.setConfigClassPath;
import static org.junit.Assert.assertTrue;

public class AccumuloMiniTest {
    Logger logger = LoggerFactory.getLogger(AccumuloMiniTest.class);
    MiniAccumuloCluster accumulo;
    PasswordToken passwd;

    @Before
    public void setup() throws Exception {
        Path tmpPath = Files.createTempDirectory("miniaccumulo");
        File tmpDir = tmpPath.toFile();
        accumulo = new MiniAccumuloCluster(tmpDir, "password");
        MiniAccumuloConfig config = accumulo.getConfig();
        // If Java 9+ then fix the classpath!
        setConfigClassPath(config);
        accumulo.start();
        logger.info("mini accumulo started");
        passwd = new PasswordToken("password");
    }

    @After
    public void teardown() throws Exception {
        accumulo.stop();
        logger.info("mini accumulo stopped");
    }

    @Test
    public void canary() throws Exception {
        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        Connector conn = instance.getConnector("root", passwd);
        logger.info("mini accumulo connected!");
        logger.info("connected as {}", conn.whoami());
    }

    @Test
    public void createTablesTest() throws Exception {
        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        Connector conn = instance.getConnector("root", passwd);
        logger.info("mini accumulo connected!");
        logger.info("connected as {}", conn.whoami());
        String tableName = "miniaccumuloTestTable";
        conn.tableOperations().create(tableName);

        Set<String> tables = conn.tableOperations().list();
        logger.info(tables.toString());
        assertTrue(tables.contains(tableName));
    }

}
