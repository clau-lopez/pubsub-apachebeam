import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class PubSubToBigQuery {
    public static void main(String[] args){

        PubSubToBigQueryOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PubSubToBigQueryOptions.class);

        options.setStreaming(true);


        //Table reference for bigquery
        TableReference tableSpec = new TableReference()
                .setProjectId(options.getProjectId())
                .setDatasetId(options.getDatasetId())
                .setTableId(options.getTableId());


        //Table schema for bigquery
        TableSchema tableSchema = new TableSchema()
                .setFields(
                        ImmutableList.of(
                                new TableFieldSchema()
                                        .setName("sunlight")
                                        .setType("INTEGER")
                                        .setMode("REQUIRED"),
                                new TableFieldSchema()
                                        .setName("temperature")
                                        .setType("FLOAT")
                                        .setMode("REQUIRED"),
                                new TableFieldSchema()
                                        .setName("moisture")
                                        .setType("INTEGER")
                                        .setMode("REQUIRED"),
                                new TableFieldSchema()
                                        .setName("fertility")
                                        .setType("INTEGER")
                                        .setMode("REQUIRED"),
                                new TableFieldSchema()
                                        .setName("idDevice")
                                        .setType("STRING")
                                        .setMode("REQUIRED"),
                                new TableFieldSchema()
                                        .setName("battery")
                                        .setType("INTEGER")
                                        .setMode("REQUIRED"),
                                new TableFieldSchema()
                                        .setName("timestamp")
                                        .setType("STRING")
                                        .setMode("REQUIRED")
                        ));


        Pipeline pipeline = Pipeline.create(options);
        pipeline

                // 1) Read string messages from a Pub/Sub topic.
                .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                // 2) Group the messages into fixed-sized minute intervals.
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
                .apply("transformation", ParDo.of(new Transformation()))
                // 3) Write one file to Big Query for every window of messages.
                .apply("Write Files to Bigquery", BigQueryIO.<SensorRecord>write()
                        .to(tableSpec)
                        .withSchema(tableSchema)
                        .withFormatFunction((SensorRecord sensorRecord) -> new TableRow()
                                .set("sunlight", sensorRecord.getSunlight())
                                .set("temperature", sensorRecord.getTemperature())
                                .set("moisture", sensorRecord.getMoisture())
                                .set("fertility", sensorRecord.getFertility())
                                .set("idDevice", sensorRecord.getIdDevice())
                                .set("battery", sensorRecord.getBattery())
                                .set("timestamp", sensorRecord.getTimestamp())
                        ).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                );

        // Execute the pipeline and wait until it finishes running.
        pipeline.run().waitUntilFinish();
    }

}