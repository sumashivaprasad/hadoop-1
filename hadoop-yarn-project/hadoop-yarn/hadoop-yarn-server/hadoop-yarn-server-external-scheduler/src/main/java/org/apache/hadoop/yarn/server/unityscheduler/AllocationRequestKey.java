package org.apache.hadoop.yarn.server.unityscheduler;

import org.apache.hadoop.yarn.api.records.Priority;

public class AllocationRequestKey {

  private String jobId;

  private String allocationRequestId;

  //TODO - Do we need priority here?
  private Priority priority;

  public String getJobId() {
    return jobId;
  }

  public String getAllocationRequestId() {
    return allocationRequestId;
  }

  public Priority getPriority() {
    return priority;
  }

  public AllocationRequestKey(String jobId, String allocationRequestId,
      Priority priority) {
    this.jobId = jobId;
    this.allocationRequestId = allocationRequestId;
    this.priority = priority;
  }
}
