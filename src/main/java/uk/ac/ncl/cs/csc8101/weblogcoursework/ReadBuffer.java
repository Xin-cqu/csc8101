package uk.ac.ncl.cs.csc8101.weblogcoursework;

/**
 * Created by xwen on 2/13/14.
 */
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ReadBuffer {


    private final File dataDir = new File("/home/xwen/Downloads");
    // 1,352,794,346 lines, 13050324662bytes (13G), md5sum=b7089321366fe6f8131196b81d060c5d
    // first line: 34600 [30/Apr/1998:21:30:17 +0000] "GET /images/hm_bg.jpg HTTP/1.0" 200 24736
    // last line:  515626 [26/Jul/1998:21:59:55 +0000] "GET /english/images/team_hm_header.gif HTTP/1.1" 200 763
    private final File logFile = new File(dataDir, "loglite");

    private final DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");


    public void readDataFile() throws IOException, ParseException {

        try (
                final FileInputStream fileInputStream = new FileInputStream(logFile);
                //final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
                final InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                final BufferedReader bufferedReader = new BufferedReader(inputStreamReader)
        ) {
            String line =null;
            long i=0;
            InsertIntoDatabase insertIntoDatabase = new InsertIntoDatabase();
            insertIntoDatabase.staticSetup();
            while((line = bufferedReader.readLine())!=null){
            //read each line of document
            final String[] tokens = line.split(" ");
            //there are four parts separated

            String dateString = tokens[1]+" "+tokens[2];
            Date date = dateFormat.parse(dateString);
            int clientId = Integer.parseInt(tokens[0]);
            Date time = date;
            String url = tokens[3]+tokens[4]+tokens[5];
            String status = tokens[6]+tokens[7];

                insertIntoDatabase.insert(clientId,time,url,status);
                if(i%10000==0){
                    System.out.println(i);
                }
                i++;
            //long millis = date.getTime();
            //System.out.println(millis+dateString);

        }
            insertIntoDatabase.staticCleanup();
        }
    }
}


