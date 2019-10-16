import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.jupiter.api.Test;


public class PipelineTest {

    @Rule
    public TestPipeline testPipeline = TestPipeline.create();
    Instant baseTime = new Instant(0);

    @Test
    void testBasicPipeline() {
        TestStream<String> createEvents =
                TestStream.create(AvroCoder.of(String.class))
                        .addElements(event("20,25.0,10,5,234:343:675,40,100000000", Duration.ZERO))
                        .advanceWatermarkToInfinity();

        PCollection<SensorRecord> records =
                testPipeline.apply(createEvents).apply(ParDo.of(new Transformation()));

        PAssert.that(records).containsInAnyOrder(new SensorRecord(20, 25.0f, 10, 5, "234:343:675", 40, "100000000"));
    }

    @Test
    void testEmptyResultInPipeline() {
        TestStream<String> createEvents =
                TestStream.create(AvroCoder.of(String.class))
                        .addElements(event("20,25.0,10,5,234:343:675,40", Duration.ZERO))
                        .advanceWatermarkToInfinity();

        PCollection<SensorRecord> records =
                testPipeline.apply(createEvents).apply(ParDo.of(new Transformation()));

        PAssert.that(records).empty();
    }

    private TimestampedValue<String> event(String record, Duration baseTimeOffset) {
        return TimestampedValue.of(record, baseTime.plus(baseTimeOffset));
    }

}
