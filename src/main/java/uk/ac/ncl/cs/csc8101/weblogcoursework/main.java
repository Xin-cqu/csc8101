package uk.ac.ncl.cs.csc8101.weblogcoursework;
import com.datastax.driver.core.*;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by xwen on 2/5/14.
 */
public class main {
    public static void main(String args[]) throws IOException, ParseException {

        PartTwo p = new PartTwo();

        p.read();

    }
}
