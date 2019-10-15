import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.ToString;

@Setter
@EqualsAndHashCode
@ToString
public class SensorRecord {

    private int sunlight;
    private float temperature;
    private int moisture;
    private int fertility;
    private String idDevice;
    private int battery;
    private String timestamp;
}
