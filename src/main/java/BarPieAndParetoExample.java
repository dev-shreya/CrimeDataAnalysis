import tech.tablesaw.api.Table;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.traces.BarTrace;
import tech.tablesaw.plotly.traces.PieTrace;

public class BarPieAndParetoExample {

    public static void main(String[] args) throws Exception {


        Table testTable = Table.read().csv("SANDAG_Crime_Data2.csv");

        Layout layout1 = Layout.builder().title("Annual average crime rate in San Diego ").build();
        PieTrace trace1 = PieTrace.builder(testTable.categoricalColumn(0), testTable.numberColumn(1)).build();
        Plot.show(new Figure(layout1, trace1));

        BarTrace btrace = BarTrace.builder(testTable.categoricalColumn(0), testTable.numberColumn(1)).build();
        Plot.show(new Figure(layout1, btrace));

        Layout marylandDataLayout = Layout.builder().title("Top Five cities in Maryland with highest crime rate ").build();
        Table MDTable = Table.read().csv("Maryland_output/part-00000");
        BarTrace btraceMD = BarTrace.builder(MDTable.categoricalColumn(0), MDTable.numberColumn(1)).build();
        Plot.show(new Figure(marylandDataLayout, btraceMD));

        Layout DCDataLayout = Layout.builder().title("Highest crime occurrences at different time of the day").build();
        Table DCTable = Table.read().csv("DC_output/part-00000");
        BarTrace btraceDC = BarTrace.builder(DCTable.categoricalColumn(0), DCTable.numberColumn(1)).build();
        Plot.show(new Figure(DCDataLayout, btraceDC));

     

    }

}