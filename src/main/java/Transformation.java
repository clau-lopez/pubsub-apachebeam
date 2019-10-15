import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transformation extends DoFn<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(Transformation.class);


    @ProcessElement
    public void processElement(@Element String information, OutputReceiver<String> out) {
        LOG.info("Information: " + information);
        LOG.debug("Information: " + information);

        String[] informationArray = information.split(",");
        if(informationArray.length == 7){
            int sunlight = Integer.parseInt(informationArray[0]);
            float temperature = Float.parseFloat(informationArray[1]);
            int moisture = Integer.parseInt(informationArray[2]);
            int fertility = Integer.parseInt(informationArray[3]);
            String idDevice = informationArray[4];
            int battery = Integer.parseInt(informationArray[5]);
            String timestamp = informationArray[6];

            SensorRecord sensorRecord = new SensorRecord();
            sensorRecord.setSunlight(sunlight);
            sensorRecord.setTemperature(temperature);
            sensorRecord.setMoisture(moisture);
            sensorRecord.setFertility(fertility);
            sensorRecord.setIdDevice(idDevice);
            sensorRecord.setBattery(battery);
            sensorRecord.setTimestamp(timestamp);

            out.output(sensorRecord.toString());
        }

    }
}
