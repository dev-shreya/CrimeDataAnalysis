import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.BarTrace;
import tech.tablesaw.plotly.traces.PieTrace;

import java.io.*;

public class VisualRepresentation {

    public static void main(String[] args) throws Exception {

        VisualRepresentation vrObject = new VisualRepresentation();
        vrObject.addHeaderToFile("SanDiego_output/part-00000", "Year,CrimeRate");
        vrObject.addHeaderToFile("LA_output/part-00000","CrimeType,Occurrence");


        Table sanDiegoTable  = Table.read().csv("SanDiego_output/part-00000");
        Layout sdDataLayout = Layout.builder().title("Annual average crime rate in San Diego ").build();
        PieTrace trace1 = PieTrace.builder(sanDiegoTable.categoricalColumn(0),sanDiegoTable.numberColumn(1)).build();
        Plot.show(new Figure(sdDataLayout, trace1));

        Layout laDataLayout = Layout.builder().title("Top Five Crime Type in LA ").build();
        Table laTable = Table.read().csv("LA_output/part-00000");
        BarTrace btrace = BarTrace.builder(laTable.categoricalColumn(0), laTable.numberColumn(1)).build();
        Plot.show(new Figure(laDataLayout, btrace));


    }

    public void addHeaderToFile(String filePath, String fileHeader) throws IOException {
        File fileObject = new File(filePath);
        FileInputStream fis = new FileInputStream(fileObject);
        BufferedReader br = new BufferedReader((new InputStreamReader(fis)));
        String result = "";
        String line = "";
        while( (line = br.readLine()) != null){
            if(!line.contains(fileHeader))
                result = result + line + '\n';
        }

        result = fileHeader + "\n" + result;

        fileObject.delete();
        FileOutputStream fos = new FileOutputStream(fileObject);
        fos.write(result.getBytes());
        fos.flush();
    }
}