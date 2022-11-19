import tech.tablesaw.api.Table;

import java.io.IOException;

//import org.apache.spark.sql.SparkSession;
public class CrimeScala {

    public static void main(String[] args) throws IOException {
        Table table1 = Table.read().csv("SANDAG_Crime_Data1.csv");
        System.out.println(table1.name());

        Table rows = table1.first(3);
        System.out.println(rows.column(0).toString());
    }

}
