/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package longtrieuuit.sparkexample;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import java.sql.SQLException;
import java.text.ParseException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 *
 * @author trieu
 */
public class Connect_SparkWithCassandra {

    private static final String CASSANDRA_DB_USER = "remoteuser";
    private static final String CASSANDRA_USER_PASS = "FWguest345!";
    private static final String TABLE_NAME = "d_registered_com_1000000005";
    private static final String KEY_SPACES = "testdb";
    private static final String CASSANDRA_HOSTS = "192.168.130.63,192.168.130.64,192.168.130.65,192.168.130.66,192.168.130.67,192.168.130.68,192.168.130.69,192.168.130.70,192.168.130.71,192.168.130.72,192.168.130.73,192.168.130.74";

    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
        SparkConf sparkConf = new SparkConf(true);
        // sparkConf.setMaster("spark://192.168.1.212:7077");
        sparkConf.setAppName("How to connect Spark with Cassandra App");
        sparkConf.set("spark.cassandra.connection.host", CASSANDRA_HOSTS);
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.cassandra.input.split.size_in_mb", "50");
        sparkConf.set("spark.broadcast.blockSize", "500M");
        sparkConf.set("spark.executor.cores", "1");
        sparkConf.set("spark.rpc.message.maxSize", "400");
        sparkConf.set("spark.driver.maxResultSize", "55g");
        sparkConf.set("spark.kryoserializer.buffer.max.mb", "500");
        sparkConf.set("spark.cassandra.auth.username", CASSANDRA_DB_USER);
        sparkConf.set("spark.cassandra.auth.password", CASSANDRA_USER_PASS);
        sparkConf.set("spark.cassandra.input.consistency.level", "TWO");
        sparkConf.set("spark.shuffle.io.maxRetries", "20");
        sparkConf.set("spark.rpc.numRetries", "30");
        sparkConf.set("spark.task.maxFailures", "30");
   //     sparkConf.set("spark.cassandra.output.consistency.level", "EACH_QUORUM");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<com.datastax.spark.connector.japi.CassandraRow> cassandraRDD = CassandraJavaUtil.javaFunctions(javaSparkContext).cassandraTable(KEY_SPACES, TABLE_NAME).select("yyyymmdd,id".split(","));
        
    // Count CassandraRow
        System.out.println("CassandraRow count: " + cassandraRDD.count());
    }

}
