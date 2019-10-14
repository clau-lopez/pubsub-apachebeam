import org.apache.beam.repackaged.direct_java.runners.fnexecution.control.RemoteOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransformationTest {

    @Test
    void shouldBeEqualWhenLightIs20() {
        Transformation transformation = new Transformation();

        String information = "20,25.0,10,5,234:343:675,4,100000000";
        SensorRecord sensorRecordExpected = new SensorRecord();
        sensorRecordExpected.setSunlight(20);

        DoFn.OutputReceiver outputReceiver = mock(DoFn.OutputReceiver.class);
        transformation.processElement(information, outputReceiver);
        verify(outputReceiver).output(sensorRecordExpected.toString());
    }
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
        verify(outputReceiver).output(sensorRecordExpected.toString());
    }



}
