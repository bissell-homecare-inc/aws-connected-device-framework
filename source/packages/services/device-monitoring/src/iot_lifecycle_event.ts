/*********************************************************************************************************************
 *  Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.                                           *
 *                                                                                                                    *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    *
 *  with the License. A copy of the License is located at                                                             *
 *                                                                                                                    *
 *      http://www.apache.org/licenses/LICENSE-2.0                                                                    *
 *                                                                                                                    *
 *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES *
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
 *  and limitations under the License.                                                                                *
 *********************************************************************************************************************/
import { AssetLibUpdate } from './assetlib_update';
import { logger } from './utils/logger';
import { container } from './di/inversify.config';
import { TYPES } from './di/types';
import _ from 'lodash';
import { LifecycleEvent } from './lifecycleEvent.model';
import async from 'async';

let assetLib: AssetLibUpdate;

async function handleMessage(message: LifecycleEvent) {
  const clientId = message.clientId;

  let connected: boolean = message.eventType === 'connected';
  // DUPLICATE_CLIENTID means the device will be reconnecting shortly, so
  // don't worry about trying to update in this scenario
  if (message.disconnectReason === 'DUPLICATE_CLIENTID') {
    return;
  }
  const currentlyConnected = await assetLib.getDeviceConnected(clientId);
  if (currentlyConnected !== undefined && connected !== currentlyConnected) {
    await assetLib.updateDeviceConnected(clientId, connected);
  }
}

/***
 * Take an array of lifecycle events and remove all but the
 * latest event for each clientId from the array
 */
function filterEventList(events: LifecycleEvent[]): LifecycleEvent[] {
  const groupedRecords: { [clientId: string]: LifecycleEvent[] } = _.groupBy(
    events,
    (record: LifecycleEvent) => record.clientId
  );
  const filteredEvents: LifecycleEvent[] = [];
  for (const message in groupedRecords) {
    // Get the latest lifecycle event
    const lifecycleEvent: LifecycleEvent = _.sortBy(groupedRecords[message], (e) => e.timestamp)[
      groupedRecords[message].length - 1
    ];

    filteredEvents.push(lifecycleEvent);
  }
  return filteredEvents;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
exports.lambda_handler = async (event: any, _context: unknown) => {
  logger.debug(`event: ${JSON.stringify(event)}`);

  if (assetLib === undefined) {
    assetLib = container.get(TYPES.AssetLibUpdate);
  }

  if ('Records' in event) {
    const records = filterEventList(event.Records);
    await async.forEachLimit(records, 5, async (record) => {
      await handleMessage(record);
    });
  } else {
    try {
      await handleMessage(event);
    } catch (e) {
      logger.debug(`Invalid event: ${JSON.stringify(event)}`);
    }
  }
};
