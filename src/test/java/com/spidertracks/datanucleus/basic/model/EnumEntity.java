/**********************************************************************
Copyright (c) 2010 Todd Nine. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/
package com.spidertracks.datanucleus.basic.model;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;

import com.spidertracks.datanucleus.model.BaseEntity;

/**
 * @author Todd Nine
 *
 */
@PersistenceCapable(table = "PrimitiveObject", identityType = IdentityType.APPLICATION)
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class EnumEntity extends BaseEntity {

	private EnumValues first;
	
	private EnumValues second;

	public EnumValues getFirst() {
		return first;
	}

	public void setFirst(EnumValues first) {
		this.first = first;
	}

	public EnumValues getSecond() {
		return second;
	}

	public void setSecond(EnumValues second) {
		this.second = second;
	}
}
