package com.continuuity.internal.pipeline;

import com.continuuity.pipeline.Pipeline;
import com.continuuity.pipeline.PipelineFactory;

/**
 * A factory for providing synchronous pipeline.
 */
public class SynchronousPipelineFactory implements PipelineFactory {

  /**
   * @return A synchronous pipeline.
   */
  @Override
  public Pipeline getPipeline() {
    return new SynchronousPipeline();
  }
}
