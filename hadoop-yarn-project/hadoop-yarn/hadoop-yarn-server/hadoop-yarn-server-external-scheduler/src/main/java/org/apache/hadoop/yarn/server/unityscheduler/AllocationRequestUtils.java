package org.apache.hadoop.yarn.server.unityscheduler;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import si.v1.Si;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp
    .RMWebServices.DEFAULT_QUEUE;

public class AllocationRequestUtils {

  public static List<Si.AllocationAsk> transformResourceRequests(
      RMContext context, ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask) {

    List<Si.AllocationAsk> allocationAsks = new ArrayList<>();

    for (ResourceRequest req : ask) {

      Si.AllocationAsk.Builder askBuilder = Si.AllocationAsk.newBuilder();
      askBuilder.setQueueName(getQueue(appAttemptId, context));

      //Allocation key
      askBuilder.setAllocationKey(appAttemptId.toString() + "_" + String
          .valueOf(req.getAllocationRequestId()));

      askBuilder.setJobId(appAttemptId.toString());

      askBuilder.setMaxAllocations(req.getNumContainers());

      //Node Label
      askBuilder.setPartitionName(AllocationRequestUtils.getPartition(req));

      //Priority
      Si.Priority priority = transformResourcePriority(req.getPriority());
      askBuilder.setPriority(priority);

      //Set resource asks
      Resource resource = req.getCapability();
      askBuilder.setResourceAsk(transformResource(resource));

      //TODO - Add NODE, RACK locality placement constraint requests
      allocationAsks.add(askBuilder.build());

    }
    return allocationAsks;
  }

  public static List<Si.AllocationAsk> transformSchedulingRequests(RMContext
      context,
      ApplicationAttemptId appAttemptId,
      List<SchedulingRequest> schedulingRequests) {

    List<Si.AllocationAsk> allocationAsks = new ArrayList<>();

    for (SchedulingRequest schedulingRequest : schedulingRequests) {
      schedulingRequest.getAllocationRequestId();

      Si.AllocationAsk.Builder askBuilder = Si.AllocationAsk.newBuilder();

      ResourceSizing resourceSizing = schedulingRequest.getResourceSizing();
      Resource resource = resourceSizing.getResources();

      Si.Resource resourceAsk = transformResource(resource);
      askBuilder.setResourceAsk(resourceAsk);

      askBuilder.setMaxAllocations(resourceSizing.getNumAllocations());
      askBuilder.setJobId(appAttemptId.toString());

      //Allocation Tags
      Set<String> allocationTags = schedulingRequest.getAllocationTags();

      for (String tag : allocationTags) {
        //TODO - What should allocation tag key/value be - set vs map?
        askBuilder.putTags(tag, tag);
      }

      askBuilder.setQueueName(getQueue(appAttemptId, context));

      //Priority
      Si.Priority priority = transformResourcePriority(
          schedulingRequest.getPriority());
      askBuilder.setPriority(priority);

      //Uncomment below when Placement constraint is ready
      //Placement constraints
      //      askBuilder.setPlacementConstraint(transformPlacementConstraint
      //          (schedulingRequest.getPlacementConstraint()));

      allocationAsks.add(askBuilder.build());
    }
    return allocationAsks;
  }

  //  private Si.PlacementConstraint transformPlacementConstraint
  //      (PlacementConstraint placementConstraintReq) {
  //    Si.PlacementConstraint.Builder result = Si
  //        .PlacementConstraint.newBuilder();
  //
  //    Si.SimplePlacementConstraint.Builder simplePlacementConstraint =
  //        Si.SimplePlacementConstraint.newBuilder();
  ////    final PlacementConstraint.AbstractConstraint constraintExpr =
  ////        placementConstraintReq.getConstraintExpr();
  //
  //    //Transform single constraint
  ////    PlacementConstraint.SingleConstraint singleConstraint =
  ////        (PlacementConstraint.SingleConstraint) constraintExpr;
  ////    String scope = singleConstraint.getScope();
  ////
  ////    simplePlacementConstraint.setAllocationAffinityAttribute
  ////        (AllocationAffinityConstraints.)
  ////     singleConstraint.getMinCardinality();
  ////    singleConstraint.getMaxCardinality();
  //
  //    //TODO: Transform composite constraint
  //    //TODO: Transform cardinality constraint
  //    //TODO: Transform timeplaced constraint
  //
  //    result.setSimpleConstraint(simplePlacementConstraint);
  //    return result.build();
  //  }

  private static String getQueue(ApplicationAttemptId appAttemptId, RMContext
      rmContext) {
    RMApp app = rmContext.getRMApps().get(appAttemptId.getApplicationId());
    return getQueue(app);
  }

  public static String getQueue(RMApp app) {
    return StringUtils.isEmpty(app.getQueue()) ?
        DEFAULT_QUEUE :
        app.getQueue();

  }

  private static Si.Priority transformResourcePriority(Priority priority) {
    Si.Priority.Builder reqPriority = Si.Priority.newBuilder();
    reqPriority.setPriorityValue(priority.getPriority());
    return reqPriority.build();
  }

  public static Si.Resource transformResource(Resource resource) {
    Si.Resource.Builder resourceBuilder = Si.Resource.newBuilder();
    for (ResourceInformation resourceInformation : resource.getResources()) {

      Si.Quantity.Builder quantityBuilder = Si.Quantity.newBuilder();
      quantityBuilder.setValue(resourceInformation.getValue());

      resourceBuilder.putResources(resourceInformation.getName(),
          quantityBuilder.build());
    }
    return resourceBuilder.build();
  }

  public static String getPartition(ResourceRequest request) {
    String partition = request.getNodeLabelExpression() == null ? NO_LABEL :
        request.getNodeLabelExpression();
    return partition;
  }

  public static String getPartition(RMApp rmApp) {
    String partition = rmApp.getAppNodeLabelExpression() == null ? NO_LABEL :
        rmApp.getAppNodeLabelExpression();
    return partition;
  }
}
