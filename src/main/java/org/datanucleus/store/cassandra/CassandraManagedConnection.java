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
package org.datanucleus.store.cassandra;

import javax.transaction.xa.XAResource;

import me.prettyprint.cassandra.service.CassandraClient;

import org.datanucleus.store.connection.AbstractManagedConnection;

/**
 * Implementation of a ManagedConnection.
 */
public class CassandraManagedConnection extends AbstractManagedConnection {

	private CassandraClient client;

	public CassandraManagedConnection(CassandraClient client) {
		this.client = client;
	}

	@Override
	public void close() {
		// do nothing, handled internally via hector

	}

	@Override
	public XAResource getXAResource() {
		// don't support xa, return null
		return null;
	}

	/**
	 * Return the hector client
	 */
	@Override
	public Object getConnection() {
		return client;
	}

}