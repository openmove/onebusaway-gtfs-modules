/**
 * Copyright (C) 2011 Brian Ferris <bdferris@onebusaway.org>
 * Copyright (C) 2012 Google, Inc.
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
package org.onebusaway.gtfs.serialization;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.onebusaway.csv_entities.CsvEntityContext;
import org.onebusaway.csv_entities.CsvEntityReader;
import org.onebusaway.csv_entities.CsvInputSource;
import org.onebusaway.csv_entities.CsvTokenizerStrategy;
import org.onebusaway.csv_entities.EntityHandler;
import org.onebusaway.csv_entities.schema.DefaultEntitySchemaFactory;
import org.onebusaway.gtfs.impl.GtfsDaoImpl;
import org.onebusaway.gtfs.model.Agency;
import org.onebusaway.gtfs.model.AgencyAndId;
import org.onebusaway.gtfs.model.Area;
import org.onebusaway.gtfs.model.Block;
import org.onebusaway.gtfs.model.FareAttribute;
import org.onebusaway.gtfs.model.FareRule;
import org.onebusaway.gtfs.model.FeedInfo;
import org.onebusaway.gtfs.model.Frequency;
import org.onebusaway.gtfs.model.IdentityBean;
import org.onebusaway.gtfs.model.Level;
import org.onebusaway.gtfs.model.Note;
import org.onebusaway.gtfs.model.Pathway;
import org.onebusaway.gtfs.model.Ridership;
import org.onebusaway.gtfs.model.Route;
import org.onebusaway.gtfs.model.ServiceCalendar;
import org.onebusaway.gtfs.model.ServiceCalendarDate;
import org.onebusaway.gtfs.model.ShapePoint;
import org.onebusaway.gtfs.model.Stop;
import org.onebusaway.gtfs.model.StopTime;
import org.onebusaway.gtfs.model.Transfer;
import org.onebusaway.gtfs.model.Translation;
import org.onebusaway.gtfs.model.Trip;
import org.onebusaway.gtfs.model.Zone;
import org.onebusaway.gtfs.services.GenericMutableDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GtfsReader extends CsvEntityReader {

	private final Logger _log = LoggerFactory.getLogger(GtfsReader.class);

	public static final String KEY_CONTEXT = GtfsReader.class.getName()
			+ ".context";

	private List<Class<?>> _entityClasses = new ArrayList<>();

	private final GtfsReaderContextImpl _context = new GtfsReaderContextImpl();

	private GenericMutableDao _entityStore = new GtfsDaoImpl();

	private List<Agency> _agencies = new ArrayList<>();

	private final Map<Class<?>, Map<String, String>> _agencyIdsByEntityClassAndId = new HashMap<>();

	private String _defaultAgencyId;

	private final Map<String, String> _agencyIdMapping = new HashMap<>();

	private boolean _overwriteDuplicates = false;

	public GtfsReader() {

		this._entityClasses.add(Agency.class);
		this._entityClasses.add(Block.class);
		this._entityClasses.add(ShapePoint.class);
		this._entityClasses.add(Note.class);
		this._entityClasses.add(Area.class);
		this._entityClasses.add(Route.class);
		this._entityClasses.add(Level.class);
		this._entityClasses.add(Stop.class);
		this._entityClasses.add(Trip.class);
		this._entityClasses.add(StopTime.class);
		this._entityClasses.add(ServiceCalendar.class);
		this._entityClasses.add(ServiceCalendarDate.class);
		this._entityClasses.add(FareAttribute.class);
		this._entityClasses.add(FareRule.class);
		this._entityClasses.add(Frequency.class);
		this._entityClasses.add(Pathway.class);
		this._entityClasses.add(Transfer.class);
		this._entityClasses.add(FeedInfo.class);
		this._entityClasses.add(Ridership.class);
		this._entityClasses.add(Translation.class);
		this._entityClasses.add(Zone.class);

		CsvTokenizerStrategy tokenizerStrategy = new CsvTokenizerStrategy();
		tokenizerStrategy.getCsvParser().setTrimInitialWhitespace(true);
		this.setTokenizerStrategy(tokenizerStrategy);

		this.setTrimValues(true);

		/**
		 * Prep the Entity Schema Factories
		 */
		DefaultEntitySchemaFactory schemaFactory = this.createEntitySchemaFactory();
		this.setEntitySchemaFactory(schemaFactory);

		CsvEntityContext ctx = this.getContext();
		ctx.put(GtfsReader.KEY_CONTEXT, this._context);

		this.addEntityHandler(new EntityHandlerImpl());
	}

	public void setLastModifiedTime(Long lastModifiedTime) {
		if (lastModifiedTime != null) {
			this.getContext().put("lastModifiedTime", lastModifiedTime);
		}
	}
	public Long getLastModfiedTime() {
		return (Long)this.getContext().get("lastModifiedTime");
	}

	public List<Agency> getAgencies() {
		return this._agencies;
	}

	public void setAgencies(List<Agency> agencies) {
		this._agencies = new ArrayList<>(agencies);
	}

	public void setDefaultAgencyId(String feedId) {
		this._defaultAgencyId = feedId;
	}

	public String getDefaultAgencyId() {
		if (this._defaultAgencyId != null) {
			return this._defaultAgencyId;
		}
		if (this._agencies.size() > 0) {
			return this._agencies.get(0).getId();
		}
		throw new NoDefaultAgencyIdException();
	}

	public void addAgencyIdMapping(String fromAgencyId, String toAgencyId) {
		this._agencyIdMapping.put(fromAgencyId, toAgencyId);
	}

	public GtfsReaderContext getGtfsReaderContext() {
		return this._context;
	}

	public GenericMutableDao getEntityStore() {
		return this._entityStore;
	}

	public void setEntityStore(GenericMutableDao entityStore) {
		this._entityStore = entityStore;
	}

	public List<Class<?>> getEntityClasses() {
		return this._entityClasses;
	}

	public void setEntityClasses(List<Class<?>> entityClasses) {
		this._entityClasses = entityClasses;
	}

	public void setOverwriteDuplicates(boolean overwriteDuplicates) {
		this._overwriteDuplicates = overwriteDuplicates;
	}

	public void run() throws IOException {
		this.run(this.getInputSource());
	}

	public void run(CsvInputSource source) throws IOException {

		List<Class<?>> classes = this.getEntityClasses();

		this._entityStore.open();

		for (Class<?> entityClass : classes) {
			this._log.info("reading entities: " + entityClass.getName());

			this.readEntities(entityClass, source);
			this._entityStore.flush();
		}

		this._entityStore.close();
	}

	/****
	 * Protected Methods
	 ****/

	protected DefaultEntitySchemaFactory createEntitySchemaFactory() {
		return GtfsEntitySchemaFactory.createEntitySchemaFactory();
	}

	protected Object getEntity(Class<?> entityClass, Serializable id) {
		if (entityClass == null) {
			throw new IllegalArgumentException("entity class must not be null");
		}
		if (id == null) {
			throw new IllegalArgumentException("entity id must not be null");
		}
		return this._entityStore.getEntityForId(entityClass, id);
	}

	protected String getTranslatedAgencyId(String agencyId) {
		String id = this._agencyIdMapping.get(agencyId);
		if (id != null) {
			return id;
		}
		return agencyId;
	}

	protected String getAgencyForEntity(Class<?> entityType, String entityId) {

		Map<String, String> agencyIdsByEntityId = this._agencyIdsByEntityClassAndId.get(entityType);

		if (agencyIdsByEntityId != null) {
			String id = agencyIdsByEntityId.get(entityId);
			if (id != null) {
				return id;
			}
		}

		throw new EntityReferenceNotFoundException(entityType, entityId);
	}


	/****
	 * Private Internal Classes
	 ****/

	private class EntityHandlerImpl implements EntityHandler {

		@Override
		public void handleEntity(Object entity) {

			if (entity instanceof Agency) {
				Agency agency = (Agency) entity;
				if (agency.getId() == null) {
					if (GtfsReader.this._defaultAgencyId == null) {
						agency.setId(agency.getName());
					} else {
						agency.setId(GtfsReader.this._defaultAgencyId);
					}
				}

				// If we already have this agency from a previous load, then we don't
				// add it or save it to the entity store
				if (GtfsReader.this._agencies.contains(agency)) {
					return;
				}

				GtfsReader.this._agencies.add((Agency) entity);
			} else if (entity instanceof Pathway) {
				Pathway pathway = (Pathway) entity;
				this.registerAgencyId(Pathway.class, pathway.getId());
			} else if (entity instanceof Level) {
				Level level = (Level) entity;
				this.registerAgencyId(Level.class, level.getId());
			} else if (entity instanceof Route) {
				Route route = (Route) entity;
				this.registerAgencyId(Route.class, route.getId());
			} else if (entity instanceof Trip) {
				Trip trip = (Trip) entity;
				this.registerAgencyId(Trip.class, trip.getId());
			} else if (entity instanceof Stop) {
				Stop stop = (Stop) entity;
				this.registerAgencyId(Stop.class, stop.getId());
			} else if (entity instanceof FareAttribute) {
				FareAttribute fare = (FareAttribute) entity;
				this.registerAgencyId(FareAttribute.class, fare.getId());
			} else if (entity instanceof Note) {
				Note note = (Note) entity;
				this.registerAgencyId(Note.class, note.getId());
			} else if (entity instanceof Area) {
				Area area = (Area) entity;
				this.registerAgencyId(Area.class, area.getId());
			}

			if (entity instanceof IdentityBean<?>) {
				GtfsReader.this._entityStore.saveEntity(entity);
			}

		}

		private void registerAgencyId(Class<?> entityType, AgencyAndId id) {

			Map<String, String> agencyIdsByEntityId = GtfsReader.this._agencyIdsByEntityClassAndId.get(entityType);

			if (agencyIdsByEntityId == null) {
				agencyIdsByEntityId = new HashMap<>();
				GtfsReader.this._agencyIdsByEntityClassAndId.put(entityType, agencyIdsByEntityId);
			}

			if (agencyIdsByEntityId.containsKey(id.getId()) && !GtfsReader.this._overwriteDuplicates) {
				throw new DuplicateEntityException(entityType, id);
			}

			agencyIdsByEntityId.put(id.getId(), id.getAgencyId());
		}
	}

	private class GtfsReaderContextImpl implements GtfsReaderContext {

		@Override
		public Object getEntity(Class<?> entityClass, Serializable id) {
			return GtfsReader.this.getEntity(entityClass, id);
		}

		@Override
		public String getDefaultAgencyId() {
			return GtfsReader.this.getDefaultAgencyId();
		}

		@Override
		public List<Agency> getAgencies() {
			return GtfsReader.this.getAgencies();
		}

		@Override
		public String getAgencyForEntity(Class<?> entityType, String entityId) {
			return GtfsReader.this.getAgencyForEntity(entityType, entityId);
		}

		@Override
		public String getTranslatedAgencyId(String agencyId) {
			return GtfsReader.this.getTranslatedAgencyId(agencyId);
		}
	}
}
