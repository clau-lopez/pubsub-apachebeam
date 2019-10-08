import org.apache.beam.sdk.transforms.DoFn;

public class Transformation extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        if (c.element().startsWith("A")) {
            c.output(c.element() + " - starts with a ");
        } else {
            c.output(c.element() + " - does not starts with a ");
        }
    }
}
