package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.datastax.driver.core.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by xwen on 2/5/14.
 */
public class PartOne {
    private static Cluster cluster;
    private static Session session;
    final DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");

    public  void staticSetup() {

        cluster = new Cluster.Builder()
                .addContactPoint("127.0.0.1")
                .build();

        final Session bootstrapSession = cluster.connect();
        bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS partone WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
        bootstrapSession.shutdown();

        session = cluster.connect("partone");
        //Assume that a client cannot access different urls at the same time, so pick clientId and accessTime together as primary key.
        session.execute("CREATE TABLE IF NOT EXISTS partone_data_table (clientId int, time timestamp, url text, PRIMARY KEY (clientId,time,url) )");
        //session.execute("CREATE TABLE IF NOT EXISTS partone_counter_table (k bigint, v counter, PRIMARY KEY (k) )");
    }
    public  void staticCleanup() {
        session.shutdown();
        cluster.shutdown();
    }
    public  void insert(int clientId,String time, String url) throws ParseException {
        Date date = dateFormat.parse(time);
        final PreparedStatement insertPs=session.prepare("INSERT INTO partone_data_table (clientId,time,url) VALUES (?, ?, ?)");
        session.execute(new BoundStatement(insertPs).bind(clientId,date,url));
    }
    public  void showUrl(int clientId, String start, String end) throws IOException {
        final PreparedStatement selectPS = session.prepare("SELECT url FROM partone_data_table WHERE time>=? AND time<=? ALLOW FILTERING");
        System.out.println("Time format is: [dd/MMM/yyyy:HH:mm:ss z]");
        System.out.println("Start time is: ");
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(System.in));
        start=bufferedReader.readLine();
        System.out.println("End time is: ");
        BufferedReader bufferedReader2=new BufferedReader(new InputStreamReader(System.in));
        end=bufferedReader2.readLine();
        ResultSet resultSet = session.execute(new BoundStatement(selectPS).bind(start, end));
        System.out.println("URLs you have accessed are: ");
        for(Row row:resultSet){
           String s = row.getString(0);

            System.out.println(s);
        }
     }
    public void showNum(String url,String start, String end) throws IOException {
        final PreparedStatement selectPS = session.prepare("SELECT * FROM partone_data_table WHERE url=? time>=? AND time<=? ALLOW FILTERING");
        int i=0;
        System.out.println("Time format is: [dd/MMM/yyyy:HH:mm:ss z]");
        System.out.println("Start time is: ");
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(System.in));
        start=bufferedReader.readLine();
        System.out.println("End time is: ");
        BufferedReader bufferedReader1=new BufferedReader(new InputStreamReader(System.in));
        end=bufferedReader1.readLine();
        System.out.println("Url is: ");
        BufferedReader bufferedReader2=new BufferedReader(new InputStreamReader(System.in));
        start=bufferedReader2.readLine();
        ResultSet resultSet = session.execute(new BoundStatement(selectPS).bind(url,start, end));
        System.out.println("URLs you have accessed are: ");
        for(Row row:resultSet){
            i++;
        }
        System.out.println("The number of access time is: "+i);
    }
}
