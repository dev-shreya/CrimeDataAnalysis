import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.api.HorizontalBarPlot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.BarTrace;
import tech.tablesaw.plotly.traces.PieTrace;

import java.io.*;

;

public class BarPieAndParetoExample {

    public static void main(String[] args) throws Exception {

        BarPieAndParetoExample brObject = new BarPieAndParetoExample();
        brObject.addHeaderToFile("SanDiego_output/part-00000", "Year,CrimeRate");
        brObject.addHeaderToFile("Maryland_output/part-00000","City,Occurrence");
        brObject.addHeaderToFile("DC_output/part-00000","Time,Occurrence");


        Table sanDiegoTable  = Table.read().csv("SanDiego_output/part-00000");
        Layout sdDataLayout = Layout.builder().title("Annual average crime rate in San Diego ").build();
        PieTrace trace1 = PieTrace.builder(sanDiegoTable.categoricalColumn(0),sanDiegoTable.numberColumn(1)).build();
        Plot.show(new Figure(sdDataLayout, trace1));

//-----------------------------------Bar plot for Maryland crime ------------------------------------------

        Layout MDDataLayout = Layout.builder().title("Top Five cities with highest crime in Maryland ").build();
        Table MDTable = Table.read().csv("Maryland_output/part-00000");
        BarTrace btraceMD = BarTrace.builder(MDTable.categoricalColumn(0), MDTable.numberColumn(1)).build();
        Plot.show(new Figure(MDDataLayout, btraceMD));

//        Layout layoutMD = Layout.builder().title("Top Five cities with highest crime in Maryland ").build();
//        Table MDTable= Table.read().csv("Maryland_output/part-00000");
//        Plot.show(ScatterPlot.create("Top Five cities with highest crime in Maryland", MDTable, "City",
//                "Occurrence"));
//        Plot.show(Histogram.create("Top Five cities with highest crime in Maryland", MDTable, "City"));
//        Plot.show(
//                BubblePlot.create("Top Five cities with highest crime in Maryland\"",
//                        MDTable,				// table
//                        "City",  	// x
//                        "Occurrence", 				// y
//                        "Occurrence ")); 		// bubble size
//        Plot.show(
//                HorizontalBarPlot.create(
//                        "Crime at different times of the day in Washington DC", MDTable, "City", "Occurrence"));

//        Layout DCDataLayout = Layout.builder().title("Crime at different times of the day in Washington DC ").build();
//        Table DCTable = Table.read().csv("DC_output/part-00000");
//        BarTrace btraceDC = BarTrace.builder(DCTable.categoricalColumn(0), DCTable.numberColumn(1)).build();
//        Plot.show(new Figure(DCDataLayout, btraceDC));

//        Table DCPieTable  = Table.read().csv("DC_output/part-00000");
//        Layout DCPieLayout = Layout.builder().title("Crime at different times of the day in Washington DC").build();
//        PieTrace tracePie = PieTrace.builder(DCPieTable.categoricalColumn(0),DCPieTable.numberColumn(1)).build();
//        Plot.show(new Figure(DCPieLayout, tracePie));

//       ----------------------------Horizontal Bar Chart Graph for DC --------------------------------
        Table DC_Table= Table.read().csv("DC_output/part-00000");
        Plot.show(
                HorizontalBarPlot.create(
                        "Crime at different times of the day in Washington DC", DC_Table, "Time", "Occurrence"));


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
