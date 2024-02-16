package biz.svyatoslav.learning.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

// This example is based on https://github.com/eugenp/tutorials/tree/master/persistence-modules/hbase
// Millions of "Thank You" to Eugen for providing an example that really works :)

public class SimpleHBaseApp {

    public static void main(String[] args) throws IOException {
        // We need such a strange approach to load config via "this.getClass()"
        new SimpleHBaseApp().connect();
    }

    private void connect() throws IOException {
        // See "resources/hbase-site.xml"!
        // Make sure tu put correct ip of your HBase docker container there!
        // And check if HBase is running :).
        // On the first run you'll probably get "can not resolve HOSTNAME" exception,
        // in this case put into /etc/hosts the ip:HOSTNAME for your docker container.

        Configuration config = HBaseConfiguration.create();
        String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
        config.addResource(new Path(path));

        try {
            HBaseAdmin.available(config);
            System.out.println("HBase is accessible!");
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running." + e.getMessage());
            return;
        }

        HBaseClientOperations HBaseClientOperations = new HBaseClientOperations();
        HBaseClientOperations.run(config);
    }

}