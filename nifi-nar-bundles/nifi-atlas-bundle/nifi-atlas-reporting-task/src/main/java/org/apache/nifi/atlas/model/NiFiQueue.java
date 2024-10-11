/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.atlas.model;

import org.apache.atlas.model.instance.AtlasEntity;

import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class NiFiQueue extends NiFiComponent {

    public NiFiQueue(String destinationComponentId, String namespace) {
        super(TYPE_NIFI_QUEUE, destinationComponentId, namespace);

        setName("queue");
        setDescription("Input queue for " + destinationComponentId);
    }

    public NiFiQueue(AtlasEntity atlasEntity) {
        super(atlasEntity);
    }
}
