import org.apache.beam.sdk.transforms.DoFn;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class TransformationTest {

    @Test
    void shouldCreateOutputWithTemperature() {
        Transformation transformation = new Transformation();

        String information = "20,25.0,10,5,234:343:675,40,100000000";
        SensorRecord sensorRecordExpected = new SensorRecord();
        sensorRecordExpected.setSunlight(20);
        sensorRecordExpected.setTemperature(25.0f);
        sensorRecordExpected.setMoisture(10);
        sensorRecordExpected.setFertility(5);
        sensorRecordExpected.setIdDevice("234:343:675");
        sensorRecordExpected.setBattery(40);
        sensorRecordExpected.setTimestamp("100000000");

        DoFn.OutputReceiver outputReceiver = mock(DoFn.OutputReceiver.class);
        transformation.processElement(information, outputReceiver);
        verify(outputReceiver).output(sensorRecordExpected);
    }

    @Test
    void shouldNotCreateOutput() {
        Transformation transformation = new Transformation();

        String information = "20,25.0,10,5,234:343:675,40";


        DoFn.OutputReceiver outputReceiver = mock(DoFn.OutputReceiver.class);
        transformation.processElement(information, outputReceiver);
        verifyZeroInteractions(outputReceiver);

    }


}
