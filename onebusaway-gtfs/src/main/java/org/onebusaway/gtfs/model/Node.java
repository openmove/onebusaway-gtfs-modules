/**
 * Copyright (C) 2021 Cambridge Systematics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.gtfs.model;

import org.onebusaway.csv_entities.schema.annotations.CsvField;
import org.onebusaway.csv_entities.schema.annotations.CsvFields;

/**
 * GTFS Extension representing network topology
 */
@CsvFields(filename = "nodes.txt", required = false)
public final class Node extends IdentityBean<Integer> {
	private static final long serialVersionUID = 1L;

	@CsvField(ignore = true)
	private int id;

	@CsvField(name = "origin_id")
	private String originId;

	@CsvField(name = "destination_id")
	private String destinationId;

	@CsvField(name = "transit_id")
	private String transitId;

	@CsvField(name = "km_distance")
	private int kmDistance;

	@CsvField(name = "instr_id")
	private String routingId;

	@Override
	public Integer getId() {
		return Integer.valueOf(this.id);
	}

	@Override
	public void setId(Integer id) {
		this.id = id.intValue();
	}

	public String getOriginId() {
		return this.originId;
	}

	public void setOriginId(String originId) {
		this.originId = originId;
	}

	public String getDestinationId() {
		return this.destinationId;
	}

	public void setDestinationId(String destinationId) {
		this.destinationId = destinationId;
	}

	public String getTransitId() {
		return this.transitId;
	}

	public void setTransitId(String transitId) {
		this.transitId = transitId;
	}

	public String getRoutingId() {
		return this.routingId;
	}

	public void setRoutingId(String routingId) {
		this.routingId = routingId;
	}

	public int getKmDistance() {
		return this.kmDistance;
	}

	public void setKmDistance(int kmDistance) {
		this.kmDistance = kmDistance;
	}

	@Override
	public String toString() {
		return "<Node " + this.getId() + ">";
	}
}
