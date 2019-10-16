import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

@RunWith(JUnit4.class)
public class PipelineTest  implements Serializable {

    @Rule public TestPipeline testPipeline = TestPipeline.create();

    private Instant baseTime = new Instant(0);
    private static final Duration WINDOW_DURATION = Duration.standardMinutes(5);
    private static final Duration ALLOWED_LATENESS = Duration.standardMinutes(20);

    @Test
    public void testBasicPipeline() {
        TestStream<String> createEvents =
                TestStream.create(AvroCoder.of(String.class))
                        .addElements(event("20,25.0,10,5,234:343:675,40,100000000", Duration.ZERO))
                        .advanceWatermarkToInfinity();

        PCollection<SensorRecord> records =
                testPipeline.apply(createEvents).apply(ParDo.of(new Transformation()));

        PAssert.that(records).containsInAnyOrder(new SensorRecord(20, 25.0f, 10, 5, "234:343:675", 40, "100000000"));
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testEmptyResultInPipeline() {
        TestStream<String> createEvents =
                TestStream.create(AvroCoder.of(String.class))
                        .addElements(event("20,25.0,10,5,234:343:675,40", Duration.ZERO))
                        .advanceWatermarkToInfinity();

        PCollection<SensorRecord> records =
                testPipeline.apply(createEvents).apply(ParDo.of(new Transformation()));

        PAssert.that(records).empty();
        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testEmptyResultInPipelineWhenLateData() {
        BoundedWindow window = new IntervalWindow(baseTime, WINDOW_DURATION);
        TestStream<String> createEvents =
                TestStream.create(AvroCoder.of(String.class))
                        //.advanceWatermarkTo(baseTime.plus(ALLOWED_LATENESS))
                        .advanceWatermarkTo(window.maxTimestamp())
                        .addElements(event("20,25.0,10,5,234:343:675,40,100000000", Duration.standardMinutes(14)))
                        .advanceWatermarkToInfinity();

        PCollection<SensorRecord> records =
                testPipeline
                        .apply(createEvents)
                        .apply(Window.<String>into(FixedWindows.of(WINDOW_DURATION)).withAllowedLateness(ALLOWED_LATENESS).accumulatingFiredPanes())
                        .apply(ParDo.of(new Transformation()));


        //PAssert.that(records).inWindow(window).empty();
        PAssert.that(records).inWindow(window).containsInAnyOrder(new SensorRecord(20, 25.0f, 10, 5, "234:343:675", 40, "100000000"));

        testPipeline.run().waitUntilFinish();
    }

    private TimestampedValue<String> event(String record, Duration baseTimeOffset) {
        return TimestampedValue.of(record, baseTime.plus(baseTimeOffset));
    }

}
