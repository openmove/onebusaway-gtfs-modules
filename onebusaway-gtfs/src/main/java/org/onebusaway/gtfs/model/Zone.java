/**
 * Copyright (C) 2017 OpenMove,
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

@CsvFields(filename = "zones.txt", prefix = "zone_", required = false)
public final class Zone extends IdentityBean<String> {
	private static final long serialVersionUID = 9113627137996962050L;

	@CsvField()
	private String id;

	@CsvField
	private float lat;

	@CsvField
	private float lon;

	@CsvField(optional = true)
	private String name;

	public Zone() {

	}

	public Zone(Zone z) {
		this.id = z.id;
		this.lat = z.lat;
		this.lon = z.lon;
		this.name = z.name;
	}

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public void setId(String id) {
		this.id = id;
	}

	public float getLat() {
		return this.lat;
	}

	public void setLat(float zoneLat) {
		this.lat = zoneLat;
	}

	public float getLon() {
		return this.lon;
	}

	public void setLon(float lng) {
		this.lon = lng;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
