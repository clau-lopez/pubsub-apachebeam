import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@Setter
@Getter
@EqualsAndHashCode
@ToString
@DefaultCoder(AvroCoder.class)
public class SensorRecord {

    private int sunlight;
    private float temperature;
    private int moisture;
    private int fertility;
    private String idDevice;
    private int battery;
    private String timestamp;
}
