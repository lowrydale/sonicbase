/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.sonicbase.query.DatabaseException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lowryda on 3/30/17.
 */
public class SpotRequest {

  public static void main(String[] args) throws IOException, InterruptedException {
    File keysFile = new File(System.getProperty("user.home"), ".awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(".awskeys file not found");
    }
    BasicAWSCredentials awsCredentials = null;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
      String accessKey = reader.readLine();
      String secretKey = reader.readLine();

      awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
    }

    AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard()
        .withRegion(Regions.US_EAST_1)
        .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
        .build();

    //servers
    requestInstances(ec2, "sonicbase-server", "4-dale", "0.20", 4, "ami-0c94f6d107c7019fa", //"ami-2cd41851", //ami-a12c15da (2k iops)
        "r4.2xlarge", "us-east-1b", true);

    //clients
    requestInstances(ec2, "sonicbase-client", "4-dale", "0.8", 8, "ami-0c94f6d107c7019fa", //"ami-9ed519e3",
        "r4.xlarge", "us-east-1b", false);

  }

  private static void requestInstances(AmazonEC2 ec2, String tag, String cluster, String price, int count,
                                       String ami, String type, String availabilityZone, boolean createElasticIp) throws InterruptedException {
    RequestSpotInstancesRequest requestRequest = new RequestSpotInstancesRequest();

    requestRequest.setSpotPrice(price);
    requestRequest.setInstanceCount(count);

    LaunchSpecification launchSpecification = new LaunchSpecification();
    launchSpecification.setImageId(ami);
    launchSpecification.setInstanceType(type);
    requestRequest.setAvailabilityZoneGroup(null);
    requestRequest.setLaunchGroup(null);
    SpotPlacement placement = new SpotPlacement(availabilityZone);
    launchSpecification.setPlacement(placement);
    //launchSpecification.setSubnetId("subnet-59baef3c"); //vpc - c
    //launchSpecification.setSubnetId("subnet-cacba2e6"); //vpc - d
    launchSpecification.setSubnetId("subnet-989c54c5"); //vpc - b


//    ArrayList<String> securityGroups = new ArrayList<String>();
//    securityGroups.add("sg-b0b1cdcf");
//    launchSpecification.setSecurityGroups(securityGroups);

    requestRequest.setLaunchSpecification(launchSpecification);

    RequestSpotInstancesResult requestResult = ec2.requestSpotInstances(requestRequest);

    for (SpotInstanceRequest requestResponse : requestResult.getSpotInstanceRequests()) {
      System.out.println(requestResponse.getState());
    }

    ArrayList<String> spotInstanceRequestIds = new ArrayList<String>();
    for (SpotInstanceRequest requestResponse : requestResult.getSpotInstanceRequests()) {
      System.out.println("Created Spot Request: " + requestResponse.getSpotInstanceRequestId());
      spotInstanceRequestIds.add(requestResponse.getSpotInstanceRequestId());
    }

    List<String> instanceIds = new ArrayList<>();
    boolean anyOpen = false; // assume no requests are still open
    do {
      anyOpen = false;
      DescribeSpotInstanceRequestsRequest describeRequest =
          new DescribeSpotInstanceRequestsRequest();
      describeRequest.setSpotInstanceRequestIds(spotInstanceRequestIds);

      try {
        // Get the requests to monitor
        DescribeSpotInstanceRequestsResult describeResult =
            ec2.describeSpotInstanceRequests(describeRequest);

        List<SpotInstanceRequest> describeResponses =
            describeResult.getSpotInstanceRequests();

        // are any requests open?
        for (SpotInstanceRequest describeResponse : describeResponses) {
          if (describeResponse.getState().equals("open")) {
            anyOpen = true;
            break;
          }
          // get the corresponding instance ID of the spot request
          instanceIds.add(describeResponse.getInstanceId());
        }
      } catch (AmazonServiceException e) {
        // Don't break the loop due to an exception (it may be a temporary issue)
        e.printStackTrace();
        anyOpen = true;
      }

      try {
        Thread.sleep(1000); // sleep 60s.
      } catch (Exception e) {
      }
    } while (anyOpen);

    ArrayList<Tag> requestTags = new ArrayList<Tag>();
    requestTags.add(new Tag(tag, cluster));

    CreateTagsRequest createTagsRequest_requests = new CreateTagsRequest();
    createTagsRequest_requests.setResources(instanceIds);
    createTagsRequest_requests.setTags(requestTags);

    ec2.createTags(createTagsRequest_requests);


    int assignedElasticCount = 0;
    for (String instanceId : instanceIds) {
      while (true) {
        DescribeInstanceStatusRequest describeInstanceRequest =
            new DescribeInstanceStatusRequest().withInstanceIds(instanceId);
        DescribeInstanceStatusResult describeInstanceResult = ec2.describeInstanceStatus(describeInstanceRequest);
        List<InstanceStatus> state = describeInstanceResult.getInstanceStatuses();
        while (state.size() < 1) {
          describeInstanceResult = ec2.describeInstanceStatus(describeInstanceRequest);
          state = describeInstanceResult.getInstanceStatuses();
        }
        String status = state.get(0).getInstanceState().getName();
        if (status.equalsIgnoreCase("running")) {
          break;
        }
        Thread.sleep(500);
      }

      if (assignedElasticCount++ == 0 && createElasticIp) {
        System.out.println("associating elastic ip: instance=" + instanceId);

        AllocateAddressRequest allocate_request = new AllocateAddressRequest()
            .withDomain(DomainType.Vpc);

        AllocateAddressResult allocate_response =
            ec2.allocateAddress(allocate_request);

        String allocation_id = allocate_response.getAllocationId();

        AssociateAddressRequest associate_request =
            new AssociateAddressRequest()
                .withInstanceId(instanceId)
                .withAllocationId(allocation_id);

        AssociateAddressResult associate_response =
            ec2.associateAddress(associate_request);
      }
    }
  }
}