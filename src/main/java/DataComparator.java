import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

public class DataComparator implements Comparator<Tuple2<String, Float>>, Serializable {

    @Override
    public int compare(Tuple2<String, Float> o1, Tuple2<String, Float> o2) {
        if(o1._1().compareTo(o2._1()) > 0 ) return 1;
        else if (o1._1().compareTo(o2._1()) > 0 ) return -1 ;
        else return 0;
       // return 0;
    }
}
