package biz.svyatoslav.learning.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBaseClientOperations {

    // Drop table if this value is set to true.
    static boolean INITIALIZE_FIRST = true;

    private final TableName siteUsersTableName = TableName.valueOf("site_users");
    private final String columnsFamilyPersonalData = "personal_data";
    private final String columnsFamilyPreferences = "preferences";

    private final byte[] row1 = Bytes.toBytes("u1");
    private final byte[] row2 = Bytes.toBytes("u2");
    private final byte[] row3 = Bytes.toBytes("u3");
    private final byte[] login = Bytes.toBytes("login");
    private final byte[] password = Bytes.toBytes("password");
    private final byte[] email = Bytes.toBytes("email");
    private final byte[] system = Bytes.toBytes("system");

    public void run(Configuration config) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config)) {

            Admin admin = connection.getAdmin();
            if (INITIALIZE_FIRST) {
                deleteTable(admin);
            }

            if (!admin.tableExists(siteUsersTableName)) {
                createTable(admin);
            }

            Table siteUsersTable = connection.getTable(siteUsersTableName);
            put(admin, siteUsersTable);
            get(siteUsersTable);
            scan(siteUsersTable);
            filters(siteUsersTable);
            delete(siteUsersTable);
        }
    }


    private void deleteTable(Admin admin) throws IOException {
        System.out.print("Deleting 'site_users' table... ");
        if (admin.tableExists(siteUsersTableName)) {
            admin.disableTable(siteUsersTableName);
            admin.deleteTable(siteUsersTableName);
        }
        System.out.println("Done.");
    }

    private void createTable(Admin admin) throws IOException {
        System.out.print("Creating 'site_users' table... ");
        HTableDescriptor desc = new HTableDescriptor(siteUsersTableName);
        desc.addFamily(new HColumnDescriptor(columnsFamilyPersonalData));
        desc.addFamily(new HColumnDescriptor(columnsFamilyPreferences));
        admin.createTable(desc);
        System.out.println("Done.");
    }

    private void put(Admin admin, Table table) throws IOException {
        System.out.print("Inserting data into 'site_users' table... ");

        Put p = new Put(row1);
        p.addImmutable(columnsFamilyPersonalData.getBytes(), login, Bytes.toBytes("user1"));
        p.addImmutable(columnsFamilyPersonalData.getBytes(), password, Bytes.toBytes("password1"));
        p.addImmutable(columnsFamilyPersonalData.getBytes(), email, Bytes.toBytes("user1@email.com"));
        p.addImmutable(columnsFamilyPreferences.getBytes(), system, Bytes.toBytes("Metric"));
        table.put(p);

        p = new Put(row2);
        p.addImmutable(columnsFamilyPersonalData.getBytes(), login, Bytes.toBytes("user2"));
        p.addImmutable(columnsFamilyPersonalData.getBytes(), password, Bytes.toBytes("password2"));
        p.addImmutable(columnsFamilyPersonalData.getBytes(), email, Bytes.toBytes("user2@email.com"));
        p.addImmutable(columnsFamilyPreferences.getBytes(), system, Bytes.toBytes("Metric!!!"));
        table.put(p);

        p = new Put(row3);
        p.addImmutable(columnsFamilyPersonalData.getBytes(), login, Bytes.toBytes("user3"));
        p.addImmutable(columnsFamilyPersonalData.getBytes(), password, Bytes.toBytes("password3"));
        p.addImmutable(columnsFamilyPersonalData.getBytes(), email, Bytes.toBytes("user3@email.com"));
        p.addImmutable(columnsFamilyPreferences.getBytes(), system, Bytes.toBytes("Imperial"));
        table.put(p);

        admin.disableTable(siteUsersTableName);
        try {
            HColumnDescriptor desc = new HColumnDescriptor(row1);
            admin.addColumn(siteUsersTableName, desc);
            System.out.print("Success! ");
        } catch (Exception e) {
            System.out.println("\nFailed! ");
            System.out.println(e.getMessage());
        } finally {
            admin.enableTable(siteUsersTableName);
        }

        System.out.println("Done.");
    }

    private void get(Table table) throws IOException {
        System.out.println("Getting some data from 'site_users' table... ");

        Get g = new Get(row1);
        Result r = table.get(g);
        byte[] value = r.getValue(columnsFamilyPersonalData.getBytes(), login);

        System.out.println("Fetched value: " + Bytes.toString(value));
        System.out.println("Done.");
    }

    private void scan(Table table) throws IOException {
        System.out.println("Scanning 'site_users' table... ");

        Scan scan = new Scan();
        scan.addColumn(columnsFamilyPersonalData.getBytes(), login);

        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner)
                System.out.println("Found row: " + result);
        }
        System.out.println("Done.");
    }

    private void filters(Table table) throws IOException {
        System.out.println("Scanning 'site_users' table with filter... ");
        Filter filter1 = new PrefixFilter(row1);
        Filter filter2 = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(
                login));

        List<Filter> filters = Arrays.asList(filter1, filter2);

        Scan scan = new Scan();
        scan.setFilter(new FilterList(Operator.MUST_PASS_ALL, filters));

        try (ResultScanner scanner = table.getScanner(scan)) {
            int i = 0;
            for (Result result : scanner) {
                System.out.println("Filter " + scan.getFilter() + " matched row: " + result);
                i++;
            }
        }
        System.out.println("Done.");
    }

    private void delete(Table table) throws IOException {
        final byte[] rowToBeDeleted =  Bytes.toBytes("strange_mega_user");
        System.out.println("Inserting and deleting some data with 'site_users' table... ");

        System.out.println("Inserting a data to be deleted later...");
        Put put = new Put(rowToBeDeleted);
        put.addColumn(columnsFamilyPersonalData.getBytes(), login, Bytes.toBytes("strange_mega_user_login"));
        table.put(put);

        Get get = new Get(rowToBeDeleted);
        Result result = table.get(get);
        byte[] value = result.getValue(columnsFamilyPersonalData.getBytes(), login);
        System.out.println("Fetching the data: " + Bytes.toString(value));

        System.out.println("Deleting the data...");
        Delete delete = new Delete(rowToBeDeleted);
        delete.addColumn(columnsFamilyPersonalData.getBytes(), login);
        table.delete(delete);

        result = table.get(get);
        value = result.getValue(columnsFamilyPersonalData.getBytes(), login);
        System.out.println("Fetching the data: " + Bytes.toString(value));

        System.out.println("Done. ");
    }
}