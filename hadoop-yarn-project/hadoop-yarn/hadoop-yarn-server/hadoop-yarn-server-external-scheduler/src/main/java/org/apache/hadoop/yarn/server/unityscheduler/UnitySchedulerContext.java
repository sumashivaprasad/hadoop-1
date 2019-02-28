package org.apache.hadoop.yarn.server.unityscheduler;

import javafx.application.Application;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import si.v1.Si;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.concurrent.locks.ReentrantReadWriteLock.*;

public class UnitySchedulerContext {

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private WriteLock writeLock = lock.writeLock();
  private ReadLock readLock = lock.readLock();

  public void addAcceptedJob(String jobId, ApplicationAttemptId
      applicationAttemptId) {
    try {
      writeLock.lock();
      acceptedJobs.put(jobId, applicationAttemptId);
    } finally {
      writeLock.unlock();
    }
  }

  public void removeAcceptedJob(String jobId) {
    try {
      writeLock.lock();
      acceptedJobs.remove(jobId);
    } finally {
      writeLock.unlock();
    }
  }

  private Map<String, ApplicationAttemptId> acceptedJobs = new
      HashMap<String, ApplicationAttemptId>();

  private Map<String, UnitySchedulerAppAttempt> pendingAllocations = new
      HashMap<>();


  boolean isJobAccepted(String jobId) {
    try {
      readLock.lock();
      return acceptedJobs.containsKey(jobId);
    } finally {
      readLock.unlock();
    }
  }

  public UnitySchedulerAppAttempt getPendingAllocationAttempt
      (String jobId) {

    try {
      readLock.lock();
      return pendingAllocations.get(jobId);
    } finally {
      readLock.unlock();
    }
  }

  public void addPendingAllocationIfNotExists(String jobId,
      UnitySchedulerAppAttempt appAttempt) {

    try {
      writeLock.lock();

      if (!pendingAllocations.containsKey(jobId)) {
        pendingAllocations.put(jobId, appAttempt);
      }
    } finally {
      writeLock.unlock();
    }
  }

  public ApplicationAttemptId getApplicationAttemptId(String jobId) {
    return acceptedJobs.get(jobId);
  }
}
