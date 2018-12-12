package cz.plajt.beamviz;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.values.PValue;

/**
 * Converts given {@link Pipeline Pipeline's} structure into graph description in
 * <a href="https://graphviz.gitlab.io/_pages/doc/info/lang.html">dot language</a>.
 */
public class PipelineToDot {
  private static final String DEFAULT_GRAPH_NAME = "pipeline";

  public static String convert2Dot(Pipeline pipeline){
    return PipelineToDot.convert2Dot(pipeline, DEFAULT_GRAPH_NAME);
  }

  public static String convert2Dot(Pipeline pipeline, String graphName){
    Visitor visitor = new Visitor(graphName);
    pipeline.traverseTopologically(visitor);
    return visitor.getGeneratedDot();
  }


  private static class Visitor implements PipelineVisitor {
    private final static String INDENT_STR = "   ";

    private final String graphName;
    private Node skippedEnclosingComposite;

    private StringBuilder sb = new StringBuilder();

    private String indent = "";

    private Visitor(String graphName){
      this.graphName = graphName;
    }

    @Override
    public void enterPipeline(Pipeline p) {
      // nothing to do
      sb.append("digraph ")
          .append(graphName == null | graphName.isEmpty() ? DEFAULT_GRAPH_NAME : graphName)
          .append("{\n");
    }

    @Override
    public CompositeBehavior enterCompositeTransform(Node node) {

      if(node.getTransform() == null && skippedEnclosingComposite == null){
        skippedEnclosingComposite = node;
        return CompositeBehavior.ENTER_TRANSFORM;
      }

      sb.append(indent).append("subgraph cluster_").append(normalizeClusterName(node.getFullName())).append(" {\n");
      incIdent();

      Class<?> transformClass = (node.getTransform()  != null)?node.getTransform().getClass(): node.getClass();

      sb.append(indent).append(String.format("label=\"%s(%s)\";\n", node.getFullName(), transformClass.getSimpleName()));
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(Node node) {
      if(node == skippedEnclosingComposite){
        return;
      }

      decIdent();
      sb.append(indent).append("}\n");
    }

    @Override
    public void visitPrimitiveTransform(Node node) {
      String name = normalizeName(node.getFullName());

      sb.append(indent).append(name).append(";\n");

//      // outgoing edges
      for(PValue value : node.getOutputs().values()){
        sb.append(indent).append(name).append(" -> ").append(normalizeName(value.getName())).append(";\n");
      }


      // incoming edges
      //TODO do we need this ?
      for(PValue value : node.getInputs().values()){
        sb.append(indent).append(normalizeName(value.getName())).append(" -> ").append(name).append(";\n");
      }

    }

    @Override
    public void visitValue(PValue value, Node producer) {
      // do nothing
    }

    @Override
    public void leavePipeline(Pipeline pipeline) {
      sb.append("}\n");
    }


    private void incIdent(){
      indent = indent + INDENT_STR;
    }

    private void decIdent(){
      indent = indent.substring(0, indent.length() - INDENT_STR.length());
    }

    private String normalizeName(String name){
      return String.format("\"%s\"", name);
    }

    private String normalizeClusterName(String clusterName){
      return clusterName.replaceAll("[^A-Za-z0-9]", "_");
    }

    private String getGeneratedDot(){
      return sb.toString();
    }
  }
}
