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


    }
}