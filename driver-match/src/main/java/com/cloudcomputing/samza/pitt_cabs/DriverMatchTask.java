package com.cloudcomputing.samza.pitt_cabs;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

  // driverId -> latitude, longitude
  private KeyValueStore<String, String> driverLoc;
  // blockId -> driverId, driverId, driverId...
  private KeyValueStore<String, String> driverList;
  // driverId -> Y/N
  private KeyValueStore<String, String> driverAvailability;

  private Integer getSqrDis(Integer x1, Integer y1, Integer x2, Integer y2) {
	return (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2);
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) throws Exception {
	// Initialize kv stores
	driverLoc = (KeyValueStore<String, String>) context.getStore("driver-loc");
	driverList = (KeyValueStore<String, String>) context.getStore("driver-list");
	driverAvailability = (KeyValueStore<String, String>) context.getStore("driver-availability");
  }

  @Override
  @SuppressWarnings("unchecked")
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
	String incomingStream = envelope.getSystemStreamPartition().getStream();

	if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {

	  Map<String, Object> driver_loc = (Map<String, Object>) envelope.getMessage();

	  // update location of this driver
	  Integer blockId = (Integer) driver_loc.get("blockId");
	  Integer driverId = (Integer) driver_loc.get("driverId");
	  Integer latitude = (Integer) driver_loc.get("latitude");
	  Integer longitude = (Integer) driver_loc.get("longitude");

	  driverLoc.put(driverId, latitude.toString() + ":" longitude.toString());
	}

	else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
	  Map<String, Object> event = (Map<String, Object>)envelope.getMessage();

	  String type = (String) event.get("type");
	  Integer blockId = (Integer) event.get("blockId");
	  Integer latitude = (Integer) event.get("latitude");
	  Integer longitude = (Integer) event.get("longitude");

	  if (type.equals("RIDE_REQUEST")) {
		// find a driver for this rider
		Integer riderId = (Integer) event.get("riderId");
		String[] drivers = driverList.get(blockId.toString()).split(":");

		Integer minSqrDis = new Integer(Integer.MAX_VALUE);
		String driverIdClosest = "0";

		// find closest available driver
		for (String driverId: drivers) {
		  if (driverAvailability.get(driverId.toString()).equals("Y")) {
			String[] loc = driverLoc.get(driverId.toString()).split(":");

			Integer latitude2 = Integer.parseInt(loc[0]);
			Integer longitude2 = Integer.parseInt(loc[1]);

			Integer sqrDis = getSqrDis(latitude, longitude, latitude2, longitude2);
			if (sqrDis < minSqrDis) {
			  minSqrDis = sqrDis;
			  driverIdClosest = driverId;
			}
		  }
		}

		HashMap<String, Object> match = new HashMap<String, Object>();

		match.put("riderId", riderId);
		match.put("driverId", Integer.parseInt(driverIdClosest));

		collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, match));
	  }

	  else if (type.equals("RIDE_COMPLETE")) {
		// update availability of this driver
		Integer driverId = (Integer) event.get("driverId");

		driverAvailability.put(driverId.toString(), "Y");
	  }

	  else if (type.equals("ENTERING_BLOCK")) {
		// update state of this driver
		String status = (String) event.get("status");
		Integer driverId = (Integer) event.get("driverId");

		if (status.equals("AVAILABLE")) {
		  driverAvailability.put(driverId.toString(), "Y");
		}
		else {
		  driverAvailability.put(driverId.toString(), "N");
		}

		driverList.put(blockId.toString(), driverList.get(blockId.toString()) + ":" + driverId.toString());
	  }

	  else if (type.equals("LEAVING_BLOCK")) {
		// update state of this driver
		String status = (String) event.get("status");
		Integer driverId = (Integer) event.get("driverId");

		if (status.equals("AVAILABLE")) {
		  driverAvailability.put(driverId.toString(), "Y");
		}
		else {
		  driverAvailability.put(driverId.toString(), "N");
		}

		String[] drivers = driverList.get(blockId.toString()).split(":");
		String newDrivers = "";

		// find closest available driver
		for (String driverId2: drivers) {
		  if (!driverId.equals(driverId2)) {
			newDrivers += (driverId2 + ":");
		  }
		}

		driverList.put(blockId.toString(), newDrivers.substring(0, newDrivers.length() - 1));
	  }

	  else {
		throw new IllegalStateException("Unexpected input stream: " + incomingStream);
	  }
	}
	
	else {
	  throw new IllegalStateException("Unexpected input stream: " + incomingStream);
	}
  }


  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
	//this function is called at regular intervals, not required for this project
  }
}
