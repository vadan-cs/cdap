package com.continuuity.app.logging;

import com.continuuity.common.logging.ApplicationLoggingContext;

/**
 *
 */
public class ProcedureLoggingContext extends ApplicationLoggingContext {

  public static final String TAG_PROCEDURE_ID = "procedureId";

  /**
   * Constructs the ProcedureLoggingContext
   * @param accountId account id
   * @param applicationId application id
   * @param procedureId flow id
   */
  public ProcedureLoggingContext(final String accountId,
                               final String applicationId,
                               final String procedureId) {
    super(accountId, applicationId);
    setSystemTag(TAG_PROCEDURE_ID, procedureId);
  }

  @Override
  public String getLogPartition() {
    return super.getLogPartition() + String.format(":%s", getSystemTag(TAG_PROCEDURE_ID));
  }

  @Override
  public String getLogPathFragment() {
    return String.format("%s/procedure-%s", super.getLogPathFragment(), getSystemTag(TAG_PROCEDURE_ID));
  }
}
