package uk.ac.ncl.cs.csc8101.weblogcoursework;
import com.datastax.driver.core.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;

/**
 * Created by xwen on 2/5/14.
 */
public class main {
    public static void main(String args[]) throws IOException, ParseException {

            ReadBuffer readBuffer=new ReadBuffer();
             readBuffer.readDataFile();//insert example data into database
        PartOne partOne=new PartOne();
        int id;
        String start,end,url;
        System.out.println("Give client ID: ");
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(System.in));
        id=Integer.parseInt(bufferedReader.readLine());
        System.out.println("Give start time: ");
        BufferedReader bufferedReader1=new BufferedReader(new InputStreamReader(System.in));
        start=bufferedReader1.readLine();
        System.out.println("Give end time: ");
        BufferedReader bufferedReader2=new BufferedReader(new InputStreamReader(System.in));
        end=bufferedReader2.readLine();
        partOne.showUrl(id,start,end);//for a given client id, start time and end time, show all activity by the client between the times.
        System.out.println("Give url: ");
        BufferedReader bufferedReader4=new BufferedReader(new InputStreamReader(System.in));
        url=bufferedReader4.readLine();
        partOne.showNum(url,start,end);//for a given set of URLs, start hour and end hour, show the total number of accesses for each url during the period.

        //insert the result into database
            PartTwo p = new PartTwo();
            p.read();//This method is used to read line from document and insert the result into database
            int clientId;
            System.out.println("Give client ID: ");
            BufferedReader bufferedReader3=new BufferedReader(new InputStreamReader(System.in));
            clientId=Integer.parseInt(bufferedReader3.readLine());
            p.query(clientId);//based on the client ID for each client session, returns the start time, end time, number of accesses and approximate number of distinct URLs accessed during the session.
    }
}
