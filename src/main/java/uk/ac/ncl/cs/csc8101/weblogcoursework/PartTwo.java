package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by xwen on 2/19/14.
 */
public class PartTwo {
    private static Cluster cluster;
    private static Session session;
    long i=0,j=0;
    private PreparedStatement insertPs;
    private Map<String, SiteSession> sessions = new LinkedHashMap<>();



    public static void staticSetup() {

        cluster = new Cluster.Builder()
                .addContactPoint("127.0.0.1")
                .build();

        final Session bootstrapSession = cluster.connect();
        bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS parttwo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
        bootstrapSession.shutdown();

        session = cluster.connect("parttwo");
        //Assume that a client cannot access different urls at the same time, so pick clientId and accessTime together as primary key.
        session.execute("CREATE TABLE IF NOT EXISTS parttwo_data_table (clientId int, starttime timestamp, endtime timestamp,totalhit bigint, totalurl bigint, PRIMARY KEY (clientId,starttime) )");
        //session.execute("CREATE TABLE IF NOT EXISTS partone_counter_table (k bigint, v counter, PRIMARY KEY (k) )");

    }
    public static void staticCleanup() {
        session.shutdown();
        cluster.shutdown();
    }
    public void read()throws IOException, ParseException{
        PartTwo.staticSetup();
        insertPs=session.prepare("INSERT INTO parttwo_data_table (clientId,starttime,endtime,totalhit,totalurl) VALUES (?, ?, ?, ?, ?)");

        HashMap<String,SiteSession> sessions = new LinkedHashMap<String,SiteSession>() {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                SiteSession siteSession = (SiteSession)eldest.getValue();
                boolean shouldExpire = siteSession.isExpired();
                if(shouldExpire) {
                    SiteSession  oldSession = siteSession;

                    session.execute(new BoundStatement(insertPs).bind(Integer.parseInt(oldSession.getId()),new Date(oldSession.getFirstHitMillis()),new Date(oldSession.getLastHitMillis()),oldSession.getHitCount(),oldSession.getHyperLogLog().cardinality() ));
                    j++;
                }
                return siteSession.isExpired();
            }
        };

         final File dataDir = new File("/data/cassandra-test-dataset");
        // 1,352,794,346 lines, 13050324662bytes (13G), md5sum=b7089321366fe6f8131196b81d060c5d
        // first line: 34600 [30/Apr/1998:21:30:17 +0000] "GET /images/hm_bg.jpg HTTP/1.0" 200 24736
        // last line:  515626 [26/Jul/1998:21:59:55 +0000] "GET /english/images/team_hm_header.gif HTTP/1.1" 200 763


         final File logFile = new File(dataDir, "CSC8101-logfile.gz");

        final DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");





        try (
                final FileInputStream fileInputStream = new FileInputStream(logFile);
                final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
                final InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream);
                final BufferedReader bufferedReader = new BufferedReader(inputStreamReader)
        ) {
            String line =null;


            while((line = bufferedReader.readLine())!=null){


                //read each line of document
                final String[] tokens = line.split(" ");
                //there are four parts separated
                i++;
                String dateString = tokens[1]+" "+tokens[2];
                Date date = dateFormat.parse(dateString);
                String clientId = tokens[0];
                Date time = date;
                long millis = date.getTime();
                String url = tokens[3]+tokens[4]+tokens[5];
                String status = tokens[6]+tokens[7];
                if(sessions.containsKey(clientId)){
                    SiteSession oldSession = sessions.get(clientId);
                    SiteSession.setGlobalLastHitMillis(millis);
                    if(oldSession.isExpired()){
                        //insertPs=session.prepare("INSERT INTO parttwo_data_table (clientId,starttime,endtime,totalhit,totalurl) VALUES (?, ?, ?, ?, ?)");
                        session.execute(new BoundStatement(insertPs).bind(Integer.parseInt(clientId), new Date(oldSession.getFirstHitMillis()),new Date(oldSession.getLastHitMillis()),oldSession.getHitCount(),oldSession.getHyperLogLog().cardinality() ));
                        sessions.remove(clientId);
                        j++;

                    }else{
                     oldSession.update(millis,url);
                    }
                }else{
                  SiteSession siteSession = new SiteSession(clientId,millis,url);
                    sessions.put(clientId, siteSession);
                }
                if(i%1000000==0){
                    System.out.println("Read line: "+i+" insert: "+j);
                }

            }
            for(Map.Entry<String,SiteSession> siteSession:sessions.entrySet()){
                //insertPs=session.prepare("INSERT INTO parttwo_data_table (clientId,starttime,endtime,totalhit,totalurl) VALUES (?, ?, ?, ?, ?)");
                j++;
                session.execute(new BoundStatement(insertPs).bind(Integer.parseInt(siteSession.getValue().getId()), new Date(siteSession.getValue().getFirstHitMillis()),new Date(siteSession.getValue().getLastHitMillis()),siteSession.getValue().getHitCount(),siteSession.getValue().getHyperLogLog().cardinality() ));

            }
             System.out.println("Read line: "+i+" insert: "+j);
            PartTwo.staticCleanup();

        }


    }
}
