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

public class RelationshipInfo {

    private final String guid;

    private RelationshipState state;

    public RelationshipInfo() {
        this.guid = null;
        this.state = RelationshipState.CREATED;
    }

    public RelationshipInfo(String guid) {
        this.guid = guid;
        this.state = RelationshipState.FETCHED;
    }

    public String getGuid() {
        return guid;
    }

    public void notifyActive() {
        if (state == RelationshipState.FETCHED) {
            state = RelationshipState.AS_IS;
        }
    }

    public boolean isCreated() {
        return state == RelationshipState.CREATED;
    }

    public boolean isActive() {
        return state == RelationshipState.CREATED || state == RelationshipState.AS_IS;
    }

    private enum RelationshipState {
        FETCHED,
        CREATED,
        AS_IS
    }
}
