package cz.plajt.beamviz;

import java.util.Arrays;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PipelineToDotTest {

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @Rule
  public TemporaryFolder folder= new TemporaryFolder();


  @Test
  public void basicLinearPipelineTest(){
    PCollection<Integer> inputs = pipeline.apply(Create.of(1, 2, 3, 4, 5));

    PCollection<Integer> transformed = inputs
        .apply(FlatMapElements.into(TypeDescriptors.integers())
            .via(i -> Arrays.asList(i, 2 * i, 3 * i)))
        .apply(MapElements.into(TypeDescriptors.integers()).via(i -> i + 2));

    transformed.apply("calc sum",Combine.globally( (input) -> StreamSupport.stream(input.spliterator(), false)
    .reduce( (a, b) -> a+b).orElse(0)));

    transformed
        .apply("format", MapElements.into(TypeDescriptors.strings()).via(Object::toString))
        .apply("write to temp", TextIO.write().to(folder.getRoot().getAbsolutePath() + "/out.txt"));

    String dot = PipelineToDot.convert2Dot(pipeline);

    System.out.println(dot);

    pipeline.run();
  }

}