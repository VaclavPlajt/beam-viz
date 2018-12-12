# beam-viz
Very simple tool to graph [Beam](https://beam.apache.org/) [Pipeline](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/Pipeline.java).
Output is in [Graphviz's](https://www.graphviz.org/) dot format and can be easily rendered by `dot` command (`dot -Tpng <your-dot-file> > pipeline.png`).

# Example of use
`String dot = PipelineToDot.convert2Dot(pipeline);`

