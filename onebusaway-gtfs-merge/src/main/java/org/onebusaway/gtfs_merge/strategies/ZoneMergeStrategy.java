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
package org.onebusaway.gtfs_merge.strategies;

import org.onebusaway.gtfs.model.Zone;
import org.onebusaway.gtfs_merge.GtfsMergeContext;

/**
 * Perform custom merge of FeedInfo to indicate the special configuration of
 * a merged GTFS set.
 */
public class ZoneMergeStrategy extends
        AbstractIdentifiableSingleEntityMergeStrategy<Zone> {

  public ZoneMergeStrategy() {
    super(Zone.class);
    _duplicateScoringStrategy.addPropertyMatch("id");
  }

@Override
protected void replaceDuplicateEntry(GtfsMergeContext context, Zone oldEntity, Zone newEntity) {
	// TODO Auto-generated method stub
	
}



}
