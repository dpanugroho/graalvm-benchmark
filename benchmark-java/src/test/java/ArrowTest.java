import org.bdapro.tpch.ArrowTPCH;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ArrowTest {

    @Test
    public void ArrowReadTest(){
        double result = 0;
        try {
            result = ArrowTPCH.executeQuerySix("lineitem1k.arrow");
            assertEquals(120902, result, 1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result);
    }
}
