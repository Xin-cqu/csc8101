package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.datastax.driver.core.*;

import java.util.Date;


/**
 * Created by xwen on 2/5/14.
 */
public class PartOne {
    private static Cluster cluster;
    private static Session session;

    public  void staticSetup() {

        cluster = new Cluster.Builder()
                .addContactPoint("127.0.0.1")
                .build();

        final Session bootstrapSession = cluster.connect();
        bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS partone WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
        bootstrapSession.shutdown();

        session = cluster.connect("partone");
        //Assume that a client cannot access different urls at the same time, so pick clientId and accessTime together as primary key.
        session.execute("CREATE TABLE IF NOT EXISTS partone_data_table (clientId int, time int, url text, PRIMARY KEY (clientId,time) )");
        //session.execute("CREATE TABLE IF NOT EXISTS partone_counter_table (k bigint, v counter, PRIMARY KEY (k) )");
    }
    public  void staticCleanup() {
        session.shutdown();
        cluster.shutdown();
    }
    public  void insert(int clientId,int time, String url){
        final PreparedStatement insertPs=session.prepare("INSERT INTO partone_data_table (clientId,time,url) VALUES (?, ?, ?)");
        session.execute(new BoundStatement(insertPs).bind(clientId,time,url));
    }
    public  void showUrl(int clientId, int start, int end){
        final PreparedStatement selectPS = session.prepare("SELECT url FROM partone_data_table WHERE time>=? AND time<=? ALLOW FILTERING");
        ResultSet resultSet = session.execute(new BoundStatement(selectPS).bind(start, end));
        System.out.println("URLs you have accessed are: ");
        for(Row row:resultSet){
           String s = row.getString(0);

            System.out.println(s);
        }
     }
}
